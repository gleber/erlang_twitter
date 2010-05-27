-module(twitter_stream).

-author('jebu@jebu.net').

-define(BACKOFF, 2).

-include("twitter_client.hrl").

%%
%% Copyright (c) 2009, Jebu Ittiachen
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
%%
%% API
-export([fetch/2, fetch/3]).

%% single arg version expects url of the form http://user:password@stream.twitter.com/1/statuses/sample.json
%% this will spawn the 3 arg version so the shell is free
fetch(URL, Callback) ->
    spawn_link(twitter_stream, fetch, [URL, Callback, 1]).

%% 3 arg version expects url of the form http://user:password@stream.twitter.com/1/statuses/sample.json
%% retry - number of times the stream is reconnected
%% sleep - secs to sleep between retries.
fetch(URL, Callback, Sleep) ->
    error_logger:info_msg("Fetching: ~p~n", [URL]),
    %% setup the request to process async and have it stream the data
    %% back to this process
    try http:request(get,
		     {URL, []},
		     [],
		     [{sync, false},
		      {stream, self}]) of
	{ok, RequestId} ->
	    case receive_chunk(RequestId, Callback) of
		{ok, _} ->
		    %% stream broke normally retry
		    error_logger:info_msg("Stream broke normally, retry ~n"),
		    timer:sleep(Sleep * 1000),
		    fetch(URL, Callback, Sleep * ?BACKOFF);
		{error, unauthorized, Result} ->
		    error_logger:info_msg("Request not authorized: ~p~n", [Result]),
		    {error, Result, unauthorized};
		{error, timeout} ->
		    error_logger:info_msg("Request timed out~n"),
		    timer:sleep(Sleep * 1000),
		    fetch(URL, Callback, Sleep * ?BACKOFF);
		{_, Reason} ->
		    error_logger:info_msg("Request problem: ~p ~n", [Reason]),
		    timer:sleep(Sleep * 1000),
		    fetch(URL, Callback, Sleep * ?BACKOFF)
	    end;
	Notok ->
	    error_logger:info_msg("Request not ok: ~p~n", [Notok]),
	    timer:sleep(Sleep * 1000),
	    fetch(URL, Callback, Sleep * ?BACKOFF)
    catch
	Type:Reason ->
	    error_logger:debug_msg("Caught: ~p ~p~n", [Type, Reason]),
	    timer:sleep(Sleep * 1000),
	    fetch(URL, Callback, Sleep * ?BACKOFF)
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
    io:format("Data looks like this ~p~n", [Tweet]),
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


receive_chunk(RequestId, Callback) ->
    receive
	{http, {RequestId, {error, Reason}}} when(Reason =:= etimedout) orelse(Reason =:= timeout) ->
	    {error, timeout};
	{http, {RequestId, {{_, 401, _} = Status, Headers, _}}} ->
	    {error, unauthorized, {Status, Headers}};
	{http, {RequestId, Result}} ->
	    {error, Result};

	%% start of streaming data
	{http,{RequestId, stream_start, Headers}} ->
	    error_logger:info_msg("Streaming data start ~p ~n",[Headers]),
	    receive_chunk(RequestId, Callback);

	%% Streaming chunk of data. This is where we will be looping
	%% around, we spawn this off to a seperate process as soon as
	%% we get the chunk and go back to receiving the tweets

	%% Ignore it if it's empty.
	{http,{RequestId, stream, Data}} when Data == <<"\r\n">> ->
	    receive_chunk(RequestId, Callback);

	{http,{RequestId, stream, Data}} ->
	    {M, F, A} = Callback,
	    spawn(M, F, [A ++ [fill_status_rec(mochijson2:decode(Data))]]),
	    receive_chunk(RequestId, Callback);

	%% end of streaming data
	{http,{RequestId, stream_end, Headers}} ->
	    error_logger:info_msg("Streaming data end ~p ~n", [Headers]),
	    {ok, RequestId}

	    %% timeout
    after 60 * 1000 ->
	    {error, timeout}

    end.
