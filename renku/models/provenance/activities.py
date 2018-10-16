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
"""Represent a Git commit."""

import os

import attr
import yaml
from git import NULL_TREE

from renku.api import LocalClient
from renku.models import _jsonld as jsonld
from renku.models.cwl import WORKFLOW_STEP_RUN_TYPES
from renku.models.cwl._ascwl import CWLClass
from renku.models.cwl.types import PATH_OBJECTS

from .entities import Collection, CommitMixin, Entity, Process, Workflow
from .qualified import Association, Generation, Usage


@jsonld.s(
    type='prov:Activity',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    },
    cmp=False,
)
class Activity(CommitMixin):
    """Represent an activity in the repository."""

    _id = jsonld.ib(context='@id', kw_only=True)
    _message = jsonld.ib(context='rdfs:comment', kw_only=True)
    _was_informed_by = jsonld.ib(
        context='prov:wasInformedBy',
        kw_only=True,
    )

    part_of = attr.ib(default=None, kw_only=True)

    process = attr.ib(default=None, kw_only=True)
    outputs = attr.ib(kw_only=True)

    generated = jsonld.ib(
        context={
            '@reverse': 'prov:activity',
        }, kw_only=True, hash=False
    )

    started_at_time = jsonld.ib(
        context={
            '@id': 'prov:startedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        kw_only=True,
    )

    ended_at_time = jsonld.ib(
        context={
            '@id': 'prov:endedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        kw_only=True,
    )

    @generated.default
    def default_generated(self):
        """Calculate default values."""
        return [
            Generation(
                activity=self,
                entity=Entity(
                    commit=self.commit,
                    client=self.client,
                    submodules=self.submodules,
                    path=path,
                    parent=self,
                ),
                role=role,
            ) for path, role in self.outputs.items()
        ]

    @property
    def paths(self):
        """Return all paths in the commit."""
        return {
            item.a_path
            for item in self.commit.diff(self.commit.parents or NULL_TREE)
            # if not item.deleted_file
        }

    @classmethod
    def generate_id(cls, commit):
        """Calculate action ID."""
        return 'commit/{commit.hexsha}'.format(commit=commit)

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return self.generate_id(self.commit)

    @_message.default
    def default_message(self):
        """Generate a default message."""
        return self.commit.message

    @_was_informed_by.default
    def default_was_informed_by(self):
        """List parent actions."""
        return [{
            '@id': self.generate_id(parent),
        } for parent in self.commit.parents]

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a commit."""
        return {path: None for path in self.paths}

    @started_at_time.default
    def default_started_at_time(self):
        """Configure calculated properties."""
        return self.commit.authored_datetime.isoformat()

    @ended_at_time.default
    def default_ended_at_time(self):
        """Configure calculated properties."""
        return self.commit.committed_datetime.isoformat()

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        return self.generated

    @property
    def parents(self):
        """Return a list of parents."""
        return []

    @staticmethod
    def from_git_commit(commit, client, path=None, submodules=None):
        """Populate information from the given Git commit."""
        return from_git_commit(
            commit, client, path=None, submodules=submodules
        )


@jsonld.s(
    type='wfprov:ProcessRun',
    context={
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    cmp=False,
)
class ProcessRun(Activity):
    """A process run is a particular execution of a Process description."""

    __association_cls__ = Process

    inputs = attr.ib(kw_only=True)
    outputs = attr.ib(kw_only=True)

    generated = jsonld.ib(
        context={
            '@reverse': 'prov:activity',
        },
        kw_only=True,
        hash=False,
    )

    association = jsonld.ib(
        context='prov:qualifiedAssociation',
        default=None,
        kw_only=True,
    )

    qualified_usage = jsonld.ib(context='prov:qualifiedUsage', kw_only=True)

    @generated.default
    def default_generated(self):
        """Calculate default values."""
        # TODO refactor to remove outputs property
        return [
            Generation.from_activity_path(
                activity=self,
                path=path,
                role=role,
            ) for path, role in self.outputs.items() if path is not None
        ]

    def __attrs_post_init__(self):
        """Calculate properties."""
        if self.association is None:
            self.association = Association.from_activity(self)

        if self.path is None:
            # FIXME only works for linking directory to file
            existing_outputs = set(self.outputs.values())
            for output_id, output_path in self.iter_output_files():
                if output_id not in existing_outputs:
                    self.outputs[os.path.join(
                        next(
                            path for path, usage in self.inputs.items()
                            if usage.role == 'input_directory'
                        ), output_path
                    )] = output_id
                    break

    @inputs.default
    def default_inputs(self):
        """Guess default inputs from a process."""
        inputs = {}

        try:
            basedir = os.path.dirname(self.path)
        except TypeError:
            return inputs

        commit = self.commit
        client = self.client
        process = self.process
        hierarchy = self.submodules

        revision = '{0}^'.format(commit)

        try:
            from git import Submodule

            submodules = [
                submodule for submodule in Submodule.
                iter_items(client.git, parent_commit=commit)
            ]
        except (RuntimeError, ValueError):
            # There are no submodules assiciated with the given commit.
            submodules = []

        subclients = {
            submodule: LocalClient(
                path=(client.path / submodule.path).resolve(),
                parent=client,
            )
            for submodule in submodules
        }

        def resolve_submodules(file_, **kwargs):
            original_path = client.path / file_
            if original_path.is_symlink(
            ) or file_.startswith('.renku/vendors'):
                original_path = original_path.resolve()
                for submodule, subclient in subclients.items():
                    try:
                        subpath = original_path.relative_to(subclient.path)
                        return Usage.from_revision(
                            client=subclient,
                            path=str(subpath),
                            revision=submodule.hexsha,
                            submodules=hierarchy + [submodule.name],
                            **kwargs
                        )
                    except ValueError:
                        pass

        for input_id, input_path in process.iter_input_files(basedir):
            try:
                usage_id = self._id + '/inputs/' + input_id
                dependency = resolve_submodules(
                    input_path,
                    role=input_id,
                    id=usage_id,
                )
                if dependency is None:
                    dependency = Usage.from_revision(
                        client=client,
                        path=input_path,
                        role=input_id,
                        revision=revision,
                        id=usage_id,
                    )
                inputs[input_path] = dependency
            except KeyError:
                continue

        return inputs

    @qualified_usage.default
    def default_qualified_usage(self):
        """Generate list of used artifacts."""
        return list(self.inputs.values())

    def iter_output_files(self, commit=None):
        """Yield tuples with output id and path."""
        process = self.process

        for output in process.outputs:
            if output.type in {'stdout', 'stderr'}:
                stream = getattr(process, output.type)
                if stream:
                    yield output.id, stream
            elif output.type in PATH_OBJECTS:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in process.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                            break  # out from process.inputs
                else:
                    yield output.id, glob

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a process."""
        if self.path is None:
            return {}
        return {
            output_path: output_id
            for output_id, output_path in self.iter_output_files()
        }

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        # Outputs go first
        for output in super().nodes:

            # NOTE refactor so all outputs behave the same
            if isinstance(output.entity, Collection):
                for entity in output.entity.members:
                    yield attr.evolve(entity, parent=output)
                # yield from output.entity.members

            yield output
        # Activity itself
        yield self.association.plan
        # Input directories might not be exported otherwise
        for node in self.inputs.values():
            if (node.client.path / node.path).is_dir():
                yield node

    @property
    def parents(self):
        """Return a list of parents."""
        return self.qualified_usage


@jsonld.s(
    type='wfprov:WorkflowRun',
    context={
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    cmp=False,
)
class WorkflowRun(ProcessRun):
    """A workflow run typically contains several subprocesses."""

    __association_cls__ = Workflow

    # @reverse wfprov:wasPartOfWorkflowRun

    children = attr.ib(kw_only=True)

    _processes = jsonld.ib(
        context={
            '@reverse': 'wfprov:wasPartOfWorkflowRun',
        },
        default=attr.Factory(list),
        kw_only=True,
    )
    subprocesses = attr.ib(kw_only=True)

    outputs = attr.ib(kw_only=True)
    generated = jsonld.ib(
        context={
            '@reverse': 'prov:activity',
        },
        kw_only=True,
        hash=False,
    )

    @children.default
    def default_children(self):
        """Load children from process."""
        import yaml

        try:
            basedir = os.path.dirname(self.path)
        except TypeError:
            basedir = None

        def _load(step):
            """Load step definition."""
            if isinstance(step.run, WORKFLOW_STEP_RUN_TYPES):
                return step.run

            if self.commit:
                data = (self.commit.tree / basedir /
                        step.run).data_stream.read()
            else:
                with step.run.open('r') as f:
                    data = f.read()
            return CWLClass.from_cwl(yaml.load(data))

        return {step.id: _load(step) for step in self.process.steps}

    @subprocesses.default
    def default_subprocesses(self):
        """Load subprocesses."""
        try:
            basedir = os.path.dirname(self.path)
        except TypeError:
            return

        revision = '{0}^'.format(self.commit)

        ins = {
            dependency.role: dependency
            for path, dependency in self.inputs.items()
        }

        entities = {}
        outs = {}
        subprocesses = {}

        for step in reversed(self.process.topological_steps):
            if isinstance(step.run, WORKFLOW_STEP_RUN_TYPES):
                path = None
                process = step.run
            else:
                path = os.path.join(basedir, step.run)
                process = self.children[step.id]

            subprocess_id = self._id + '/steps/' + step.id

            inputs = {}
            for alias, source in step.in_.items():
                usage_id = subprocess_id + '/inputs/' + alias
                if source in ins:
                    dependency = ins[source]
                    inputs[dependency.path] = attr.evolve(
                        dependency,
                        role=alias,
                        id=usage_id,
                    )
                elif source in outs:
                    input_path = outs[source]
                    inputs[input_path] = Usage(
                        entity=entities[input_path],
                        role=alias,
                        id=usage_id,
                    )
                else:
                    # TODO check that it is not Path or Directory
                    pass

            subprocess_entity_commit = self.client.find_previous_commit(
                path, revision=revision
            )
            subprocess = process.create_run(
                commit=self.commit,
                client=self.client,
                part_of=self,
                process=process,
                path=path,
                inputs=inputs,
                id=subprocess_id,
                submodules=self.submodules,
            )

            subprocess.association = Association.from_activity(
                subprocess,
                commit=subprocess_entity_commit,
            )

            for output_path, source in subprocess.outputs.items():
                outs.setdefault(step.id + '/' + source, output_path)

            for generation in subprocess.generated:
                entity = generation.entity
                entities[entity.path] = entity

                if isinstance(entity, Collection):
                    entities.update(
                        **{member.path: member
                           for member in entity.members}
                    )

            subprocesses[step.id] = subprocess
            self._processes.append(subprocess)

        return subprocesses

    def iter_output_files(self, commit=None):
        """Yield tuples with output id and path."""
        commit = commit or self.commit

        tools = self.default_children()
        setattr(self, 'children', tools)

        for output in self.process.outputs:
            if output.type not in PATH_OBJECTS:
                continue

            if output.outputSource:
                step_id, _, source = output.outputSource.partition('/')
                subprocess = self.subprocesses[step_id]
                for glob, output_id in subprocess.outputs.items():
                    if output.id == output_id:
                        yield output.id, glob
                        break
            elif output.outputBinding:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in self.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                else:
                    yield output.id, glob

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a workflow."""
        return super().default_outputs()

    @generated.default
    def default_generated(self):
        """Calculate default values."""
        results = []
        for output in self.process.outputs:
            step_id, _, source = output.outputSource.partition('/')
            assert step_id in self.children

            for generated in self.subprocesses[step_id].generated:
                if generated.role == source:
                    results.append(
                        attr.evolve(
                            generated,
                            role=output.id,
                            activity=self,
                        )
                    )
                    break
            else:
                raise KeyError(output)

        return results

    @property
    def nodes(self):
        """Yield all graph nodes."""
        for subprocess in reversed(self._processes):
            if subprocess.path is None:
                # skip nodes connecting directory to file
                continue
            yield from subprocess.nodes


def from_git_commit(commit, client, path=None, submodules=None):
    """Populate information from the given Git commit."""
    cls = Activity
    process = None
    hierarchy = list(submodules) if submodules else []

    if path is None:
        for file_ in commit.stats.files.keys():
            # 1.a Find process (CommandLineTool or Workflow);
            if client.is_cwl(file_):
                if path is not None:
                    raise ValueError(file_)  # duplicate
                path = file_
                continue

    if path:
        data = (commit.tree / path).data_stream.read()
        process = CWLClass.from_cwl(yaml.load(data))

        return process.create_run(
            commit=commit,
            client=client,
            process=process,
            path=path,
            submodules=hierarchy,
        )

    return cls(commit=commit, client=client, submodules=hierarchy)
