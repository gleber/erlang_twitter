%%% -*- mode:erlang -*-
{application, twitter_client, [
    {description, "An Erlang-native Twitter client."},
    {vsn, "0.5"},
    {modules, [twitter_client, twitter_client_utils, twitter_stream]},
    {registered, []},
    {applications, [kernel, stdlib, inets]},
    {env, []}
]}.
