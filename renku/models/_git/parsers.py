# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
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
"""Parsing utilities."""

import os

import attr


@attr.s
class Range:
    """Represent parsed Git revision as an interval."""

    start = attr.ib()
    stop = attr.ib()

    @classmethod
    def rev_parse(cls, git, revision):
        """Parse revision string."""
        start, is_range, stop = revision.partition('..')
        if not is_range:
            start, stop = None, start
        elif not stop:
            stop = 'HEAD'

        return cls(
            start=git.rev_parse(start) if start else None,
            stop=git.rev_parse(stop),
        )

    def __str__(self):
        """Format range."""
        if self.start:
            return '{self.start}..{self.stop}'.format(self=self)
        return str(self.stop)


@attr.s(slots=True)
class CommitGraph:
    """Represent a Git commit graph structure."""

    @attr.s(slots=True)
    class Node:
        """Represent a node in the commit graph."""

        id = attr.ib()
        parents = attr.ib()
        generation_number = attr.ib()
        created = attr.ib()

        @property
        def hexsha(self):
            """Return the hex representation."""
            return self.id.hex()

    _GIT_PATH_SUFFIX = os.path.join('objects', 'info', 'commit-graph')

    HASH_FUNCTIONS = {
        1: 'SHA-1',
    }
    HASH_LENGTH = {1: 20}

    NO_PARENT = 0x70000000
    HAS_MORE_PARENTS = 0x80000000
    END_OF_LIST = 0x80000000

    _data = attr.ib()

    signature = attr.ib()
    version = attr.ib()
    hash_id = attr.ib()
    number_of_chunks = attr.ib()

    chunks = attr.ib()
    fanout = attr.ib()
    lookup = attr.ib()

    nodes = attr.ib(default=attr.Factory(list))

    @property
    def hash_length(self):
        """Return length of the node hash."""
        return self.HASH_LENGTH[self.hash_id]

    @chunks.default
    def visit_chunks(self):
        """Parse chunk offsets (NAME: OFFSET)."""
        data = self._data

        return {
            bytes(data[offset:offset + 4]):
            int.from_bytes(data[offset + 4:offset + 12], byteorder='big')
            for offset in range(8, 8 + self.number_of_chunks * 12, 12)
        }

    @fanout.default
    def visit_fanout(self):
        """OID Fanout (ID: {'O', 'I', 'D', 'F'}) (256 * 4 bytes).

        The ith entry, F[i], stores the number of OIDs with first
        byte at most i. Thus F[255] stores the total
        number of commits (N).
        """
        data = self._data
        offset = self.chunks[b'OIDF']

        return {
            i: int.from_bytes(data[s:s + 4], byteorder='big')
            for i, s in enumerate(range(offset, offset + 256 * 4, 4))
        }

    @lookup.default
    def visit_lookup(self):
        """OID Lookup (ID: {'O', 'I', 'D', 'L'}) (N * H bytes).

        The OIDs for all commits in the graph, sorted in ascending order.
        """
        data = self._data
        offset = self.chunks[b'OIDL']

        N = self.fanout[255]
        H = self.hash_length

        return [data[s:s + H].hex() for s in range(offset, offset + N * H, H)]

    def iter_commit_data(self):
        """Commit Data (ID: {'C', 'D', 'A', 'T' }) (N * (H + 16) bytes).

        * The first H bytes are for the OID of the root tree.

        * The next 8 bytes are for the positions of the first two parents
          of the ith commit. Stores value 0x7000000 if no parent in that
          position. If there are more than two parents, the second value
          has its most-significant bit on and the other bits store an array
          position into the Extra Edge List chunk.

        * The next 8 bytes store the generation number of the commit and
          the commit time in seconds since EPOCH. The generation number
          uses the higher 30 bits of the first 4 bytes, while the commit
          time uses the 32 bits of the second 4 bytes, along with the lowest
          2 bits of the lowest byte, storing the 33rd and 34th bit of the
          commit time.
        """
        data = self._data
        N = self.fanout[255]
        H = self.hash_length
        offset = self.chunks[b'CDAT']

        def generation_number_and_datetime(chunk):
            gn = int.from_bytes(chunk, byteorder='big')
            return gn >> (2 + 32), gn & 0x7FFFFFFFF

        def parents(chunk):
            first = int.from_bytes(chunk[:4], byteorder='big')
            if first & self.NO_PARENT:
                return

            yield first

            second = int.from_bytes(chunk[4:], byteorder='big')
            if second & self.HAS_MORE_PARENTS:
                second &= ~self.HAS_MORE_PARENTS
                assert offset_edges is not None
                yield from self.iter_extra_edge_list(second)
            elif not second & self.NO_PARENT:
                yield second

        for s in range(offset, offset + N * (H + 16), (H + 16)):
            gn, dt = generation_number_and_datetime(
                data[s + H + 8:s + H + 8 + 8]
            )
            yield self.Node(
                id=data[s:s + H],
                parents=list(parents(data[s + H:s + H + 8])),
                generation_number=gn,
                created=dt,
            )

    def iter_extra_edge_list(self, start):
        """Extra Edge List (ID: {'E', 'D', 'G', 'E'}) [Optional].

        This list of 4-byte values store the second through nth parents for
        all octopus merges. The second parent value in the commit data stores
        an array position within this list along with the most-significant bit
        on. Starting at that array position, iterate through this list of
        commit positions for the parents until reaching a value with the
        most-significant bit on. The other bits correspond to the position of
        the last parent.
        """
        offset = self.chunks.get(b'EDGE')
        data = self._data
        s = offset + start * 4

        while True:
            parent = int.from_bytes(data[s:s + 4])
            if parent & cls.END_OF_LIST:
                parent &= ~cls.END_OF_LIST
                yield parent
                break

            yield parent
            s += 4

    @classmethod
    def build(cls, stream):
        data = bytearray(stream)

        signature, version, hash_id, number_of_chunks = (
            data[0:4], int(data[4]), data[5], int(data[6])
        )

        assert version == 1
        assert signature == b'CGPH'

        return cls(
            data=data,
            signature=signature,
            version=version,
            hash_id=hash_id,
            number_of_chunks=number_of_chunks,
        )

    def commit_range(self, start, stop):
        """Find all commits that belongs to the range."""
        from collections import deque

        if not self.nodes:
            self.nodes = list(self.iter_commit_data())

        index = {node.hexsha: node for node in self.nodes}

        generation_start = index[start].generation_number if start else 0

        q = deque([(stop, index[stop])])
        seen = set()
        result = []

        while q:
            hexsha, commit = q.popleft()

            if hexsha in seen:
                continue

            seen.add(hexsha)
            result.append((hexsha, commit))

            if hexsha == start:
                continue

            for parent in commit.parents:
                node = self.nodes[parent]
                if node.generation_number >= generation_start:
                    q.append(data)

        return result if start is None or start in seen else []
