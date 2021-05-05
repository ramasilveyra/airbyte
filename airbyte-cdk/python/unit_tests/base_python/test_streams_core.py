# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from typing import Any, Iterable, List, Mapping, Union

from airbyte_cdk.base_python import Stream
from airbyte_cdk.models import AirbyteStream, SyncMode


class StreamStubFullRefresh(Stream):
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        pass


def test_as_airbyte_stream_full_refresh(mocker):
    test_stream = StreamStubFullRefresh()

    mocker.patch.object(StreamStubFullRefresh, "get_json_schema", return_value={})
    airbyte_stream = test_stream.as_airbyte_stream()

    exp = AirbyteStream(name="stream_stub_full_refresh", json_schema={}, supported_sync_modes=[SyncMode.full_refresh])
    assert exp == airbyte_stream


class StreamStubIncremental(StreamStubFullRefresh):
    def cursor_field(self) -> Union[str, List[str]]:
        return "test_cusor"


def test_as_airbyte_stream_incremental(mocker):
    test_stream = StreamStubIncremental()
    test_stream.cursor_field = "test_cursor"

    mocker.patch.object(StreamStubIncremental, "get_json_schema", return_value={})
    airbyte_stream = test_stream.as_airbyte_stream()

    exp = AirbyteStream(
        name="stream_stub_incremental",
        json_schema={},
        supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
        default_cursor_field=["test_cursor"],
        source_defined_cursor=True,
    )
    assert exp == airbyte_stream
