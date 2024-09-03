-module(utils).
-export([chash/2,get_server_id/1]).

chash(X,M) ->
    Mask = round(math:pow(2,M)) -1,
    binary:decode_unsigned(crypto:hash(sha256,integer_to_list(X))) band Mask. 

get_server_id(ID) ->
    list_to_atom(integer_to_list(ID)). 
