-module(twitter_stream).

-author('jebu@jebu.net').

-behaviour(gen_server).

%% Copyright (c) 2009-2010, Jebu Ittiachen, David N. Welton
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without modification, are
%% permitted provided that the following conditions are met:
%%
%%    1. Redistributions of source code must retain the above copyright notice, this list of
%%       conditions and the following disclaimer.
%%
%%    2. Redistributions in binary form must reproduce the above copyright notice, this list
%%       of conditions and the following disclaimer in the documentation and/or other materials
%%       provided with the distribution.
%%
%% THIS SOFTWARE IS PROVIDED BY JEBU ITTIACHEN ``AS IS'' AND ANY EXPRESS OR IMPLIED
%% WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
%% FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL JEBU ITTIACHEN OR
%% CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
%% SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
%% ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
%% NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
%% The views and conclusions contained in the software and documentation are those of the
%% authors and should not be interpreted as representing official policies, either expressed
%% or implied, of Jebu Ittiachen.


%% API
-export([start_link/1]).
-export([fetch/2, add_handler/1, add_handler/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, {local, ?MODULE}).

-define(BACKOFF, 2).

-include("twitter_client.hrl").

-record(state, {eventmgr, callback, sleep, url}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Url) ->
    gen_server:start_link(?SERVER, ?MODULE, [Url], []).

%% Add a handler to receive callbacks.

add_handler(Handler) ->
    gen_server:call(?MODULE, {add_handler, Handler}).

%% Add a handler with its own eventmanager.

add_handler(EventManager, Handler) ->
    gen_server:call(?MODULE, {add_handler, EventManager, Handler}).


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Url]) ->
    {ok, EventMgr} = gen_event:start_link(),
    gen_server:cast(?MODULE, {fetch, Url, 1}),
    {ok, #state{eventmgr = [EventMgr], sleep = 1, url = Url}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------

handle_call({add_handler, EventManager, _Handler}, _From, State) ->
    EventManList = State#state.eventmgr ++ [EventManager],
    {reply, ok, State#state{eventmgr = EventManList}};

handle_call({add_handler, Handler}, _From, State) ->
    ok = gen_event:add_handler(State#state.eventmgr, Handler, []),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({fetch, Url, Sleep}, State) ->
    fetch(Url, Sleep),
    {noreply, State#state{sleep = Sleep}};

handle_cast(Msg, State) ->
    error_logger:info_msg("~p handle_cast ~p ~n", [?MODULE, Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

%% Handle all the http streaming stuff.

handle_info({http, Rest}, State) ->
    Sleep = State#state.sleep,
    Url = State#state.url,
    case Rest of
	{_RequestId, {error, Reason}} when(Reason =:= etimedout) orelse(Reason =:= timeout) ->
	    error_logger:error_msg("Stream timed out: ~p~n", [Reason]),
	    gen_server:cast(?MODULE, {fetch, Url, Sleep * ?BACKOFF});
	{_RequestId, {{_, 401, _} = _Status, _Headers, _}} ->
	    error_logger:error_msg("Unauthorized request: ~p~n");
            %% Authorization problem: we need to really fail here, no restarts.
	{_RequestId, Result} ->
	    error_logger:error_msg("Stream failure: ~p~n", [Result]),
	    gen_server:cast(?MODULE, {fetch, Url, Sleep * ?BACKOFF});
	%% start of streaming data
	{_RequestId, stream_start, Headers} ->
	    error_logger:info_msg("Streaming data start ~p ~n",[Headers]);

	%% Streaming chunk of data. This is where we will be looping
	%% around, we spawn this off to a seperate process as soon as
	%% we get the chunk and go back to receiving the tweets

	%% Ignore it if it's empty.
	{_RequestId, stream, Data} when Data == <<"\r\n">> ->
	    ok;

	%% This is where we actually do something!
	{_RequestId, stream, Data} ->
	    Tweet = fill_status_rec(mochijson2:decode(Data)),
	    notify_all_managers(State, {tweet, Tweet});
%	    {M, F, A} = State#state.callback,
%	    spawn(M, F, [A ++ [Tweet]]);
	%% end of streaming data - we just restart in this case,
	%% because we want a never-ending stream.
	{_RequestId, stream_end, Headers} ->
	    error_logger:info_msg("Streaming data end ~p ~n", [Headers]),
	    gen_server:cast(?MODULE, {fetch, Url, Sleep * ?BACKOFF})
    end,
    {noreply, State};

handle_info(Info, State) ->
    error_logger:info_msg("~p handle_info ~p ~n", [?MODULE, Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% 3 arg version expects url of the form http://user:password@stream.twitter.com/1/statuses/sample.json
%% retry - number of times the stream is reconnected
%% sleep - secs to sleep between retries.
fetch(Url, Sleep) ->
    timer:sleep(Sleep * 1000),
    error_logger:info_msg("Fetching: ~p~n", [Url]),
    %% setup the request to process async and have it stream the data
    %% back to this process
    Res = http:request(get,
		 {Url, []},
		 [],
		 [{sync, false},
		  {stream, self}]),
    case Res of
	{ok, _RequestId} ->
	    ok;
	Notok ->
	    error_logger:info_msg("Request not ok: ~p~n", [Notok]),
	    gen_server:cast(?MODULE, {fetch, Url, Sleep * ?BACKOFF})
    end.

%%====================================================================
%% Internal functions
%%====================================================================


%% These reflect the format that a mochijson2 parsed message comes in
%% as.
fill_user_rec({struct, User}) ->
    UserRec = #user{
      id = element(2, lists:keyfind(<<"id">>, 1, User)),
      name = element(2, lists:keyfind(<<"name">>, 1, User)),
      screen_name = element(2, lists:keyfind(<<"screen_name">>, 1, User)),
      location = element(2, lists:keyfind(<<"location">>, 1, User)),
      description = element(2, lists:keyfind(<<"description">>, 1, User)),
      profile_image_url = element(2, lists:keyfind(<<"profile_image_url">>, 1, User)),
      url = element(2, lists:keyfind(<<"url">>, 1, User)),
      protected = element(2, lists:keyfind(<<"protected">>, 1, User)),
      followers_count = element(2, lists:keyfind(<<"followers_count">>, 1, User)),
      profile_background_color = element(2, lists:keyfind(<<"profile_background_color">>, 1, User)),
      profile_text_color = element(2, lists:keyfind(<<"profile_text_color">>, 1, User)),
      profile_link_color = element(2, lists:keyfind(<<"profile_link_color">>, 1, User)),
      profile_sidebar_fill_color = element(2, lists:keyfind(<<"profile_sidebar_fill_color">>, 1, User)),
      profile_sidebar_border_color = element(2, lists:keyfind(<<"profile_sidebar_border_color">>, 1, User)),
      friends_count = element(2, lists:keyfind(<<"friends_count">>, 1, User)),
      created_at = element(2, lists:keyfind(<<"created_at">>, 1, User)),
      favourites_count = element(2, lists:keyfind(<<"favourites_count">>, 1, User)),
      utc_offset = element(2, lists:keyfind(<<"utc_offset">>, 1, User)),
      time_zone = element(2, lists:keyfind(<<"time_zone">>, 1, User)),
      following = element(2, lists:keyfind(<<"following">>, 1, User)),
      notifications = element(2, lists:keyfind(<<"notifications">>, 1, User)),
      statuses_count = element(2, lists:keyfind(<<"statuses_count">>, 1, User))
     },
    UserRec.

fill_status_rec(Tweet) ->
    %% io:format("Data looks like this ~p~n", [Tweet]),
    {struct, Data} = Tweet,
    Status = #status{
      created_at =	element(2, lists:keyfind(<<"created_at">>, 1, Data)),
      id =		element(2, lists:keyfind(<<"id">>, 1, Data)),
      text =		element(2, lists:keyfind(<<"text">>, 1, Data)),
      source =		element(2, lists:keyfind(<<"source">>, 1, Data)),
      truncated =	element(2, lists:keyfind(<<"truncated">>, 1, Data)),
      in_reply_to_status_id =	element(2, lists:keyfind(<<"in_reply_to_status_id">>, 1, Data)),
      in_reply_to_user_id =	element(2, lists:keyfind(<<"in_reply_to_user_id">>, 1, Data)),
      favorited =	element(2, lists:keyfind(<<"favorited">>, 1, Data)),
      user = 		fill_user_rec(element(2, lists:keyfind(<<"user">>, 1, Data)))
     },
    Status.

%% Cycle through the list of event managers and send the message to
%% all of them.

notify_all_managers(State, Message) ->
    lists:foreach(fun(Manager) ->
			  gen_event:notify(Manager, Message)
		  end,
		  State#state.eventmgr).
