-module(node).
-import(utils, [get_server_id/1]).

-behaviour(gen_server).

% public API
-export([start/3,create/2,join/2,stabilize/1,fix_fingers/1,get_state/1,find_key/2]).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

% node state
-record(state, {super,
                m,
                identity,
                predecessor = nil,
                finger = #{}
                }).

% public functions
start(Sup_PID, M, Node_ID) ->
    gen_server:start({local,get_server_id(Node_ID)}, ?MODULE, [Sup_PID, M ,Node_ID] ,[]).

create(Zero_Node,Max_Node) ->
    gen_server:call(get_server_id(Zero_Node), {create, Zero_Node, Max_Node}).

join(New_Node, Prime_Node) ->
    gen_server:call(get_server_id(New_Node), {join, New_Node, Prime_Node}).

stabilize(Node_ID) ->
    gen_server:cast(get_server_id(Node_ID), {stabilize}).

fix_fingers(Node_ID) ->
    gen_server:cast(get_server_id(Node_ID), {fix_fingers}).

get_state(Node_ID) ->
    gen_server:call(get_server_id(Node_ID), {get_state}).

find_key(Node_ID, Key) ->
    gen_server:cast(get_server_id(Node_ID),{find_key, Key, 0}).

%util functions
strict_between(A,B,C) ->
    B > A andalso B =< C.

between(A,B,C) ->
    B > A andalso B < C. 

% private functions
closest_preceeding_node_aux(ID, My_ID, Finger, I) ->
    if
        I == 1 ->
            maps:get(1, Finger);
        true ->
            Check_Node = maps:get(I, Finger),
            Valid_One = between(My_ID, Check_Node, ID),
            Valid_Two = ID < My_ID,
            if 
                Valid_One ->
                    Check_Node;
                Valid_Two ->
                    Check_Node;
                true ->
                    closest_preceeding_node_aux(ID, My_ID, Finger, I-1)
            end
    end.
closest_preceeding_node(ID, My_ID, Finger) ->
    I = maps:size(Finger),
    closest_preceeding_node_aux(ID, My_ID, Finger, I).

find_successor_local(M, ID, My_ID, Finger) ->
    My_Succ = maps:get(1,Finger),
    Valid_One = strict_between(My_ID, ID, My_Succ),
    Valid_Two = My_ID == round(math:pow(2,M)) -1,
    if 
        Valid_One ->
            My_Succ;
        Valid_Two ->
            My_ID;
        true ->
            N_Prime = closest_preceeding_node(ID, My_ID, Finger),
            Found_Succ = gen_server:call(get_server_id(N_Prime),{find_successor, ID}),
            Found_Succ
    end.

fix_fingers_aux(My_ID,Finger,M,I) ->
    if
        I > M ->
            Finger;
        true ->
            Find_ID = My_ID + round(math:pow(2, I-1)),
            Found_Succ = find_successor_local(M,Find_ID, My_ID, Finger),
            New_Finger = maps:put(I, Found_Succ, Finger),
            fix_fingers_aux(My_ID, New_Finger, M, I+1)
    end.

% gen_server_callbacks
init([Sup_PID, M, Node_ID]) ->
    {ok, #state{super = Sup_PID,
                m = M,
                identity = Node_ID}}.
% get_id, get_successor, get_predecessor, get_finger, create, join, notify, find_successor(Node)
handle_call({get_node_id}, _From, State)->
    {reply, State#state.identity, State};
handle_call({get_successor}, _From, State)->
    {reply, maps:get(1,State#state.finger), State};
handle_call({get_predecessor}, _From, State)->
    {reply, State#state.predecessor, State};
handle_call({get_finger}, _From, State)->
    {reply, State#state.finger, State};
handle_call({get_state}, _From, State)->
    {reply,State,State};
handle_call({create_max_node, Max_Node, Zero_Node}, _From, State) ->
    FT = #{1=>Zero_Node},
    {reply,ok,State#state{identity = Max_Node,
                          predecessor = Zero_Node,
                          finger = FT}};
handle_call({create, Zero_Node, Max_Node}, _From, State)->
    ok = gen_server:call(get_server_id(Max_Node), {create_max_node, Max_Node, Zero_Node}),
    FT = #{1 => Max_Node},
    {reply, ok, State#state{predecessor = Max_Node,
                            finger = FT}};
handle_call({join, New_Node, Prime_Node}, _From, State) ->
    Succ = gen_server:call(get_server_id(Prime_Node), {find_successor, New_Node}),
    FT = #{1 => Succ},
    ok = gen_server:call(get_server_id(Succ), {notify, New_Node}),
    {reply, ok, State#state{identity = New_Node,
                            finger = FT}};
handle_call({notify, PP_Node}, _From, State) ->
    Cur_Pred = State#state.predecessor,
    if 
        Cur_Pred == nil->
            {reply,ok,State#state{predecessor=PP_Node}};
        true->
            Valid_One = between(Cur_Pred, PP_Node, State#state.identity),
            if 
                Valid_One ->
                    {reply,ok,State#state{predecessor=PP_Node}};
                true ->
                    {reply,ok,State}
            end
    end;
handle_call({find_successor, Node_ID}, _From, State) ->
    My_Succ = maps:get(1,State#state.finger),
    Valid_One = strict_between(State#state.identity, Node_ID, My_Succ),
    Valid_Two = State#state.identity == round(math:pow(2,State#state.m)) -1,
    if 
        Valid_One ->
            {reply, My_Succ, State};
        Valid_Two ->
            {reply, State#state.identity, State};
        true ->
            N_Prime = closest_preceeding_node(Node_ID, State#state.identity, State#state.finger),
            Found_Succ = gen_server:call(get_server_id(N_Prime),{find_successor, Node_ID}),
            {reply, Found_Succ, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

% find_key, stabilize, and fix_fingers
handle_cast({find_key, Key, Num_Hops},State) ->
    My_Sup = State#state.super,
    My_Succ = maps:get(1,State#state.finger),
    Valid_One = strict_between(State#state.identity, Key, My_Succ),
    if 
        Valid_One ->
            My_Sup ! {resolved, Num_Hops+1},
            {noreply, State};
        true ->
            N_Prime = closest_preceeding_node(Key, State#state.identity, State#state.finger),
            gen_server:cast(get_server_id(N_Prime),{find_key, Key, Num_Hops+1}),
            {noreply, State}
    end;
handle_cast({stabilize}, State) ->
    Curr_Succ = maps:get(1,State#state.finger),
    X = gen_server:call(get_server_id(Curr_Succ), {get_predecessor}),
    Invalid_One = X == nil,
    Invalid_Two = X == State#state.identity,
    Valid_One = between(State#state.identity,X,Curr_Succ),
    if 
        Invalid_One ->
            {noreply,State};
        Invalid_Two ->
            {noreply, State};
        Valid_One ->
            ok = gen_server:call(get_server_id(X), {notify, State#state.identity}),
            New_Finger = maps:put(1,X,State#state.finger),
            {noreply,State#state{finger=New_Finger}};
        true ->
            ok = gen_server:call(get_server_id(Curr_Succ), {notify, State#state.identity}),
            {noreply,State}
    end;
handle_cast({fix_fingers}, State) ->
    New_Finger = fix_fingers_aux(State#state.identity, State#state.finger, State#state.m, 1),
    {noreply,State#state{finger=New_Finger}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.