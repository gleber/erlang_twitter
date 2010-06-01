-module(twitter_stream_sup).
-author('davidw@dedasys.com').

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

%% returns the Spec for the supervisor (called by the
%% supervisor:start_link above)
init([Url, Callback]) ->
    io:format("RECVD ~p ~p~n", [Url, Callback]),
    Spec = [{twitter_stream,
	     {twitter_stream, start_link, [Url, Callback]}, % this gets run via apply(M, F, A)
	     transient, 2000, worker, [twitter_stream]}],
    {ok, {{one_for_one, 10, 60}, Spec}}.
