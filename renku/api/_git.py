# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Wrap Git client."""

import itertools
import os
import shutil
import sys
import tempfile
import uuid
from contextlib import contextmanager
from time import time

import attr

from renku import errors
from renku._compat import Path


def _mapped_std_streams(lookup_paths, streams=('stdin', 'stdout', 'stderr')):
    """Get a mapping of standard streams to given paths."""
    # FIXME add device number too
    standard_inos = {}
    for stream in streams:
        try:
            stream_stat = os.fstat(getattr(sys, stream).fileno())
            key = stream_stat.st_dev, stream_stat.st_ino
            standard_inos[key] = stream
        except Exception:  # FIXME UnsupportedOperation
            pass
        # FIXME if not getattr(sys, stream).istty()

    def stream_inos(paths):
        """Yield tuples with stats and path."""
        for path in paths:
            try:
                stat = os.stat(path)
                key = (stat.st_dev, stat.st_ino)
                if key in standard_inos:
                    yield standard_inos[key], path
            except FileNotFoundError:  # pragma: no cover
                pass

    return dict(stream_inos(lookup_paths)) if standard_inos else {}


def _clean_streams(repo, mapped_streams):
    """Clean mapped standard streams."""
    for stream_name in ('stdout', 'stderr'):
        stream = mapped_streams.get(stream_name)
        if not stream:
            continue

        path = os.path.relpath(stream, start=repo.workdir)
        if (path, 0) not in repo.index.entries:
            os.remove(stream)
        else:
            blob = repo.index.entries[(path, 0)].to_blob(repo)
            with open(path, 'wb') as fp:
                fp.write(blob.data_stream.read())


@attr.s
class GitCore:
    """Wrap Git client."""

    repo = attr.ib(init=False)
    """Store an instance of the Git repository."""

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        from pygit2 import GitError, Repository

        #: Create an instance of a Git repository for the given path.
        try:
            self.repo = Repository(str(self.path))
        except GitError:
            self.repo = None

    @property
    def modified_paths(self):
        """Return paths of modified files."""
        return [
            item.new_file for item in self.repo.diff().deltas if item.new_file
        ]

    @property
    def dirty_paths(self):
        """Get paths of dirty files in the repository."""
        from pygit2 import GIT_STATUS_CURRENT, GIT_STATUS_IGNORED
        repo_path = str(self.path)
        return {
            os.path.join(repo_path, filepath)
            for filepath, flags in self.repo.status().items()
            if flags not in {GIT_STATUS_CURRENT, GIT_STATUS_IGNORED}
        }

    @property
    def candidate_paths(self):
        """Return all paths in the index and untracked files."""
        from pygit2 import GIT_STATUS_CURRENT, GIT_STATUS_IGNORED
        repo_path = str(self.path)
        return [
            os.path.join(repo_path, path)
            for filepath, flags in self.repo.status().items()
            if flags != GIT_STATUS_IGNORED
        ]

    def ensure_clean(self, ignore_std_streams=False):
        """Make sure the repository is clean."""
        dirty_paths = self.dirty_paths
        mapped_streams = _mapped_std_streams(dirty_paths)

        if ignore_std_streams:
            if dirty_paths - set(mapped_streams.values()):
                _clean_streams(self.repo, mapped_streams)
                raise errors.DirtyRepository(self.repo)

        elif self.repo.is_dirty(untracked_files=True):
            _clean_streams(self.repo, mapped_streams)
            raise errors.DirtyRepository(self.repo)

    @contextmanager
    def commit(self, author_date=None):
        """Automatic commit."""
        from pygit2 import GitError, Signature
        from renku.version import __version__

        default_signature = self.repo.default_signature
        author_date = author_date or int(time())

        yield

        author = Signature(
            default_signature.name,
            default_signature.email,
            author_date,
        )
        committer = Signature(
            'renku {0}'.format(__version__),
            'renku+{0}@datascience.ch'.format(__version__),
        )

        self.repo.index.add_all()
        argv = [os.path.basename(sys.argv[0])] + sys.argv[1:]
        # Ignore pre-commit hooks since we have already done everything.
        tree = self.repo.index.write_tree()
        try:
            head = self.repo.head
            ref = head.name
            parents = [head.target]
        except GitError:
            ref = 'HEAD'
            parents = []

        self.repo.create_commit(
            ref,
            author,
            committer,
            ' '.join(argv),
            tree,
            parents,
        )
        self.repo.index.write()

    @contextmanager
    def transaction(
        self,
        clean=True,
        up_to_date=False,
        commit=True,
        ignore_std_streams=False
    ):
        """Perform Git checks and operations."""
        if clean:
            self.ensure_clean(ignore_std_streams=ignore_std_streams)

        if up_to_date:
            # TODO
            # Fetch origin/master
            # is_ancestor('origin/master', 'HEAD')
            pass

        if commit:
            with self.commit():
                yield self
        else:
            yield self

    @contextmanager
    def worktree(self, path=None, branch_name=None):
        """Create new worktree."""
        from renku._contexts import Isolation

        delete = path is None
        path = path or tempfile.mkdtemp()
        branch_name = branch_name or 'renku/run/isolation/' + uuid.uuid4().hex

        # Keep current directory relative to repository root.
        relative = Path('.').resolve().relative_to(self.path)

        self.repo.git.worktree('add', '-b', branch_name, path)

        # Reroute standard streams
        original_mapped_std = _mapped_std_streams(self.candidate_paths)
        mapped_std = {}
        for name, stream in original_mapped_std.items():
            stream_path = Path(path) / (Path(stream).relative_to(self.path))
            stream_path = stream_path.absolute()

            if not stream_path.exists():
                stream_path.parent.mkdir(parents=True, exist_ok=True)
                stream_path.touch()

            mapped_std[name] = stream_path

        _clean_streams(self.repo, original_mapped_std)

        # TODO sys.argv

        client = attr.evolve(self, path=path)
        client.repo.config_reader = self.repo.config_reader

        new_cwd = Path(path) / relative
        new_cwd.mkdir(parents=True, exist_ok=True)

        with Isolation(cwd=str(new_cwd), **mapped_std):
            yield client

        self.repo.git.merge(branch_name, ff_only=True)
        if delete:
            shutil.rmtree(path)
            self.repo.git.worktree('prune')
