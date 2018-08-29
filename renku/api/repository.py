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
"""Client for handling a local repository."""

import datetime
import os
import uuid
from contextlib import contextmanager
from subprocess import PIPE, STDOUT, call

import attr
import filelock
import yaml
from werkzeug.utils import cached_property, secure_filename

from renku._compat import Path

HAS_LFS = call(['git', 'lfs'], stdout=PIPE, stderr=STDOUT) == 0


@attr.s
class PathMixin(object):
    """Define a default path attribute."""

    path = attr.ib(converter=lambda arg: Path(arg).resolve().absolute())

    @path.default
    def _default_path(self):
        """Return default repository path."""
        from git import InvalidGitRepositoryError
        from renku.cli._git import get_git_home

        try:
            return get_git_home()
        except InvalidGitRepositoryError:
            return '.'

    @path.validator
    def _check_path(self, _, value):
        """Check the path exists and it is a directory."""
        if not (value.exists() and value.is_dir()):
            raise ValueError('Define an existing directory.')


@attr.s
class RepositoryApiMixin(object):
    """Client for handling a local repository."""

    renku_home = attr.ib(default='.renku')
    """Define a name of the Renku folder (default: ``.renku``)."""

    renku_path = attr.ib(init=False)
    """Store a ``Path`` instance of the Renku folder."""

    git = attr.ib(init=False)
    """Store an instance of the Git repository."""

    METADATA = 'metadata.yml'
    """Default name of Renku config file."""

    LOCK_SUFFIX = '.lock'
    """Default suffix for Renku lock file."""

    WORKFLOW = 'workflow'
    """Directory for storing workflow in Renku."""

    CACHE_DIR = os.path.join('.cache', 'renku')
    """Default name of cache directory."""

    def __attrs_post_init__(self):
        """Initialize computed attributes."""
        from git import InvalidGitRepositoryError, Repo

        #: Configure Renku path.
        path = Path(self.renku_home)
        if not path.is_absolute():
            path = self.path / path

        path.relative_to(path)
        self.renku_path = path

        #: Create an instance of a Git repository for the given path.
        try:
            self.git = Repo(str(self.path))
        except InvalidGitRepositoryError:
            self.git = None
        # TODO except

        #: Cached values:
        self._latest_changes = {}

    @property
    def lock(self):
        """Create a Renku config lock."""
        return filelock.FileLock(
            str(self.renku_path.with_suffix(self.LOCK_SUFFIX))
        )

    @property
    def renku_metadata_path(self):
        """Return a ``Path`` instance of Renku metadata file."""
        return self.renku_path.joinpath(self.METADATA)

    @property
    def workflow_path(self):
        """Return a ``Path`` instance of the workflow folder."""
        return self.renku_path / self.WORKFLOW

    @cached_property
    def cache_path(self):
        """Return a ``Path`` instance of the cache directory.

        NOTE: The directory is automatically created on property access.
        """
        cache_dir = self.path / self.CACHE_DIR
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @cached_property
    def cwl_prefix(self):
        """Return a CWL prefix."""
        self.workflow_path.mkdir(parents=True, exist_ok=True)  # for Python 3.5
        return str(self.workflow_path.resolve().relative_to(self.path))

    def is_cwl(self, path):
        """Check if the path is a valid CWL file."""
        return path.startswith(self.cwl_prefix) and path.endswith('.cwl')

    def find_previous_commit(self, paths, revision='HEAD'):
        """Return a previous commit for a given path."""
        file_commits = list(self.git.iter_commits(revision, paths=paths))

        if not file_commits:
            raise KeyError(
                'Could not find a file {0} in range {1}'.format(
                    paths, revision
                )
            )

        return file_commits[0]

    @contextmanager
    def with_metadata(self):
        """Yield an editable metadata object."""
        with self.lock:
            from renku.models._jsonld import asjsonld
            from renku.models.projects import Project

            metadata_path = self.renku_metadata_path

            if self.renku_metadata_path.exists():
                with metadata_path.open('r') as f:
                    source = yaml.load(f) or {}
                metadata = Project.from_jsonld(source)
            else:
                source = {}
                metadata = Project()

            yield metadata

            source.update(**asjsonld(metadata))
            with metadata_path.open('w') as f:
                yaml.dump(source, f, default_flow_style=False)

    @contextmanager
    def with_workflow_storage(self):
        """Yield a workflow storage."""
        with self.lock:
            from renku.models.cwl._ascwl import ascwl
            from renku.models.cwl.workflow import Workflow

            workflow = Workflow()
            yield workflow

            for step in workflow.steps:
                step_name = '{0}_{1}.cwl'.format(
                    uuid.uuid4().hex,
                    secure_filename('_'.join(step.run.baseCommand)),
                )

                workflow_path = self.workflow_path
                if not workflow_path.exists():
                    workflow_path.mkdir()

                step_path = workflow_path / step_name
                with step_path.open('w') as step_file:
                    yaml.dump(
                        ascwl(
                            # filter=lambda _, x: not (x is False or bool(x)
                            step.run,
                            filter=lambda _, x: x is not None,
                            basedir=workflow_path,
                        ),
                        stream=step_file,
                        default_flow_style=False
                    )

    def init_repository(
        self, name=None, force=False, use_external_storage=True
    ):
        """Initialize a local Renku repository."""
        from git import Repo

        path = self.path.absolute()
        if force:
            self.renku_path.mkdir(parents=True, exist_ok=force)
            if self.git is None:
                self.git = Repo.init(str(path))
        else:
            if self.git is not None:
                raise FileExistsError(self.git.git_dir)

            self.renku_path.mkdir(parents=True, exist_ok=force)
            self.git = Repo.init(str(path))

        self.git.description = name or path.name

        # Check that an author can be determined from Git.
        from renku.models.datasets import Author
        Author.from_git(self.git)

        # TODO read existing gitignore and create a unique set of rules
        import pkg_resources
        gitignore_default = pkg_resources.resource_stream(
            'renku.data', 'gitignore.default'
        )
        gitignore_path = path / '.gitignore'
        with gitignore_path.open('w') as gitignore:
            gitignore.write(gitignore_default.read().decode())

            gitignore.write(
                '\n' + str(
                    self.renku_path.relative_to(self.path)
                    .with_suffix(self.LOCK_SUFFIX)
                ) + '\n'
            )

        with self.with_metadata() as metadata:
            metadata.name = name
            metadata.updated = datetime.datetime.utcnow()

        # initialize LFS if it is requested and installed
        if use_external_storage and HAS_LFS:
            self.init_external_storage(force=force)

        return str(path)

    def init_external_storage(self, force=False):
        """Initialize the external storage for data."""
        cmd = ['git', 'lfs', 'install', '--local']
        if force:
            cmd.append('--force')

        call(
            cmd,
            stdout=PIPE,
            stderr=STDOUT,
            cwd=str(self.path.absolute()),
        )

    def track_paths_in_storage(self, *paths):
        """Track paths in the external storage."""
        if HAS_LFS and self.git.config_reader(config_level='repository'
                                              ).has_section('filter "lfs"'):
            # FIXME create configurable filter and respect .gitattributes
            paths = [
                path for path in paths if not str(path).endswith('.ipynb')
            ]

            call(
                ['git', 'lfs', 'track'] + list(paths),
                stdout=PIPE,
                stderr=STDOUT,
                cwd=str(self.path),
            )

    def get_index(self, revision='HEAD'):
        """Return a file index for given revision."""
        if revision == 'HEAD':
            return self.git.index

        from git import IndexFile
        return IndexFile.from_tree(self.git, revision)

    def latest_changes(self, revision='HEAD', cache=True):
        """Return revision of latest change for every file."""
        commit = self.git.rev_parse(revision)

        if cache and commit.hexsha in self._latest_changes:
            return self._latest_changes[commit.hexsha]

        cache_dir = self.cache_path / 'latest_changes'
        cache_dir.mkdir(parents=True, exist_ok=True)

        cache_file = (cache_dir / str(commit)).with_suffix('.yaml')
        changes = None

        if cache and cache_file.exists():
            try:
                with cache_file.open('r') as f:
                    changes = yaml.load(f.read())
            except Exception:  # pragma: no cover
                #: There is a problem with cached file. We'll recreate it.
                pass

        if changes is None:
            changes = {}
            index = self.get_index(revision=revision)
            paths = {path for path, _ in index.entries.keys()}
            start = new_start = commit

            while paths:
                change = 0
                delta = 10  # len(paths) // 2

                for step in self.git.iter_commits(start, paths=list(paths)):
                    #: Move the starting point for next iteration.
                    new_start = step
                    #: Calling `iter_commits` is expensive. Continue until
                    #: there are many changes.
                    if change > delta:
                        break

                    hexsha = step.hexsha
                    touched_paths = set(step.stats.files.keys())
                    paths -= touched_paths
                    change += len(touched_paths)
                    for path in touched_paths:
                        changes.setdefault(path, hexsha)

                #: No progress in the loop
                if new_start == start:
                    break

                #: Move the starting point
                start = new_start

            if cache:
                self._latest_changes[commit.hexsha] = changes
                with cache_file.open('w') as f:
                    f.write(yaml.dump(changes, default_flow_style=False))

        return changes
