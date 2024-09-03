-module(sim_sup).
-import(utils, [chash/2,get_server_id/1]).
-import(node, [start/3,create/2,join/2,stabilize/1,fix_fingers/1,get_state/1,find_key/2]).
-export([run/2,sup_start/2]).

spawn_nodes(Num_Nodes,Sup_PID,M,I) ->
    if 
        I == 0 ->
            ok;
        true ->
            Node_ID = chash(I,M),
            {ok,_}= start(Sup_PID,M,Node_ID),
            spawn_nodes(Num_Nodes,Sup_PID,M,I-1)
    end.

join_nodes(Num_Nodes,M,I) ->
    if 
        I > Num_Nodes ->
            io:format("All nodes joined~n"),
            ok;
        true ->
            Prime_Node = 0, 
            New_Node = chash(I,M),
            ok = join(New_Node,Prime_Node),
            io:format("New Node ~p joined~n", [New_Node]),
            % stabilize and fix fingers of nodes
            ok = stabilize_nodes(M,I,1),
            io:format("stabilized nodes 1 to ~p~n", [I]),
            ok = fix_fingers_of_nodes(M,I,1),
            io:format("Fixed finger of nodes 1 to ~p~n", [I]),
            join_nodes(Num_Nodes,M,I+1)
    end.

stabilize_nodes(M,L,I) ->
    if 
        I > L ->
            stabilize(0),
            ok;
        true ->
            Node_ID = chash(I,M),
            stabilize(Node_ID),
            timer:sleep(20),
            stabilize_nodes(M,L,I+1)
    end.

fix_fingers_of_nodes(M,L,I) ->
    if 
        I > L ->
            fix_fingers(0),
            ok;
        true ->
            Node_ID = chash(I,M),
            fix_fingers(Node_ID),
            timer:sleep(20),
            fix_fingers_of_nodes(M,L,I+1)
    end.

start_message_requests_aux2(M, Num_Nodes, I)->
    End = I > Num_Nodes,
    if 
        End ->
            ok;
        true ->
            Random_Key = crypto:rand_uniform(0,round(math:pow(2,M))),
            Node_ID = chash(I,M),
            find_key(Node_ID,Random_Key),
            start_message_requests_aux2(M,Num_Nodes,I+1)
    end. 

start_message_requests_aux(M, Num_Nodes, Num_Requests, Rounds)->
    End = Rounds > Num_Requests,
    if
        End ->
            io:format("~p requests made for every ~p nodes~n", [Num_Requests, Num_Nodes]),
            ok;
        true ->
            ok = start_message_requests_aux2(M,Num_Nodes,1),
            timer:sleep(100),
            start_message_requests_aux(M,Num_Nodes, Num_Requests, Rounds+1)
    end.


start_message_requests(M, Num_Nodes, Num_Requests) ->
    ok = start_message_requests_aux(M, Num_Nodes, Num_Requests, 1).


sup_loop(Num_Nodes, Num_Requests, Resolved_Count, Total_Hops) ->
    End = Resolved_Count == Num_Nodes * Num_Requests,
    if 
        End ->
            Total_Hops;
        true ->
            receive
                {resolved, Num_Hops} ->
                    % io:format("Resolved a key search with ~p Hops~n",[Num_Hops]),
                    sup_loop(Num_Nodes, Num_Requests, Resolved_Count+1,Total_Hops+Num_Hops)
            end
    end.

sup_start(Num_Nodes, Num_Requests)->
    M = 2*round(math:log2(math:pow(Num_Nodes,2))),
    ok = spawn_nodes(Num_Nodes,self(),M,Num_Nodes),
    Zero_Node = 0,
    {ok,_} = start(self(),M,Zero_Node),
    Max_Node = round(math:pow(2,M)) -1,
    {ok,_} = start(self(),M,Max_Node),
    ok = create(Zero_Node,Max_Node),
    io:format("Base Nodes ~p,~p created~n", [Zero_Node,Max_Node]),
    % join all nodes to ring
    ok = join_nodes(Num_Nodes,M,1),
    ok = start_message_requests(M, Num_Nodes,Num_Requests),
    Total_Hops = sup_loop(Num_Nodes,Num_Requests,0,0),
    io:format("~n~nTotal Hops: ~p~n",[Total_Hops]),
    Average_Hops = Total_Hops / (Num_Nodes*Num_Requests),
    io:format("Average Hops: ~p~n", [Average_Hops]).
    

run(Num_Nodes, Num_Requests) ->
    spawn(sim_sup, sup_start,[Num_Nodes,Num_Requests]).
