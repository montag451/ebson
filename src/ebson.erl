-module(ebson).

%% API exports
-export([decode/1,
         decode_document/1]).

-type bson_document() :: [bson_element()].
-type bson_element() :: {binary(), bson_value()}.
-type bson_value() :: {double, float()}
                    | {string, binary()}
                    | {document, bson_document()}
                    | {array, [bson_element()]}
                    | {binary, {generic, binary()}}
                    | {binary, {uuid_old, binary()}}
                    | {binary, {uuid, binary()}}
                    | undefined
                    | {object_id, <<_:96>>}
                    | {boolean, boolean()}
                    | {utc, integer()}
                    | null
                    | {regex, {binary(), binary()}}
                    | {int32, integer()}
                    | {uint64, pos_integer()}
                    | {int64, integer()}
                    | min_key
                    | max_key.

%%====================================================================
%% API functions
%%====================================================================

-spec decode(Bin :: binary()) -> [bson_document()].
decode(Bin) ->
    decode(Bin, []).

-spec decode_document(Bin :: binary()) -> {bson_document(), binary()}.
decode_document(<<L:32/signed-little, Rest/binary>>) when size(Rest) >= L - 4 ->
    BodyLen = L - 5,
    <<Body:BodyLen/binary, 0, Rest2/binary>> = Rest,
    {decode_elems(Body), Rest2}.

%%====================================================================
%% Internal functions
%%====================================================================

-spec decode(Bin :: binary(), [bson_document()]) -> [bson_document()].
decode(<<>>, Docs) ->
    lists:reverse(Docs);
decode(Bin, Docs) ->
    {Doc, Rest} = decode_document(Bin),
    decode(Rest, [Doc | Docs]).

-spec decode_elems(Bin :: binary()) -> [bson_element()].
decode_elems(Bin) ->
    decode_elems(Bin, []).

-spec decode_elems(Bin :: binary(), Elems :: [bson_element()]) -> [bson_element()].
decode_elems(<<Type, Rest/binary>>, Elems) ->
    {Name, Rest2} = decode_cstring(Rest),
    {Value, Rest3} = decode_value(Type, Rest2),
    decode_elems(Rest3, [{Name, Value} | Elems]);
decode_elems(<<>>, Elems) ->
    lists:reverse(Elems).

-spec decode_cstring(Bin :: binary()) -> {binary(), binary()}.
decode_cstring(Bin) ->
    decode_cstring(Bin, <<>>).

-spec decode_cstring(Bin :: binary(), binary()) -> {binary(), binary()}.
decode_cstring(<<0, Rest/binary>>, Name) ->
    {Name, Rest};
decode_cstring(<<B, Rest/binary>>, Name) ->
    decode_cstring(Rest, <<Name/binary, B>>).

-spec decode_value(Type :: pos_integer(), Bin :: binary()) -> {bson_value(), binary()}.
% double
decode_value(1, Bin) ->
    <<Double/float-little, Rest/binary>> = Bin,
    {{double, Double}, Rest};
% UTF-8 string
decode_value(2, Bin) ->
    <<L:32/signed-little, Rest/binary>> = Bin,
    L2 = L - 1,
    <<S:L2/binary, 0, Rest2/binary>> = Rest,
    {{string, S}, Rest2};
% embedded document
decode_value(3, Bin) ->
    {Doc, Rest} = decode_document(Bin),
    {{document, Doc}, Rest};
% array
decode_value(4, Bin) ->
    {Array, Rest} = decode_document(Bin),
    {{array, Array}, Rest};
% binary (generic)
decode_value(5, <<L:32/signed-little, 0, Binary:L/binary, Rest/binary>>) ->
    {{binary, {generic, Binary}}, Rest};
% binary (UUID old)
decode_value(5, <<L:32/signed-little, 3, Binary:L/binary, Rest/binary>>) ->
    {{binary, {uuid_old, Binary}}, Rest};
% binary (UUID)
decode_value(5, <<L:32/signed-little, 4, Binary:L/binary, Rest/binary>>) ->
    {{binary, {uuid, Binary}}, Rest};
% undefined
decode_value(6, Bin) ->
    {undefined, Bin};
% object id
decode_value(7, Bin) ->
    <<ObjId:12/binary, Rest/binary>> = Bin,
    {{object_id, ObjId}, Rest};
% boolean (false)
decode_value(8, <<0, Rest/binary>>) ->
    {{boolean, false}, Rest};
% boolean (true)
decode_value(8, <<1, Rest/binary>>) ->
    {{boolean, true}, Rest};
% UTC datetime
decode_value(9, <<N:64/signed-little, Rest/binary>>) ->
    {{utc, N}, Rest};
% null
decode_value(10, Bin) ->
    {null, Bin};
% regex
decode_value(11, Bin) ->
    {Regex, Rest} = decode_cstring(Bin),
    {Opts, Rest2} = decode_cstring(Rest),
    {{regex, {Regex, Opts}}, Rest2};
% int32
decode_value(16, <<N:32/signed-little, Rest/binary>>) ->
    {{int32, N}, Rest};
% uint64
decode_value(17, <<N:64/unsigned-little, Rest/binary>>) ->
    {{uint64, N}, Rest};
% int64
decode_value(18, <<N:64/signed-little, Rest/binary>>) ->
    {{int64, N}, Rest};
% min key
decode_value(255, Bin) ->
    {min_key, Bin};
% max key
decode_value(127, Bin) ->
    {max_key, Bin}.
