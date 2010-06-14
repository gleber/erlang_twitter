-module(twitter_client_utils).

-export([url_encode/1, string_to_int/1, compose_body/1, status_to_json/1, user_to_json/1]).

-include("twitter_client.hrl").

%% some utility functions stolen from yaws_api module

url_encode([H|T]) ->
    if
        H >= $a, $z >= H ->
            [H|url_encode(T)];
        H >= $A, $Z >= H ->
            [H|url_encode(T)];
        H >= $0, $9 >= H ->
            [H|url_encode(T)];
        H == $_; H == $.; H == $-; H == $/; H == $: -> % FIXME: more..
            [H|url_encode(T)];
        true ->
            case integer_to_hex(H) of
                [X, Y] ->
                    [$%, X, Y | url_encode(T)];
                [X] ->
                    [$%, $0, X | url_encode(T)]
            end
     end;

url_encode([]) -> [].

integer_to_hex(I) ->
    case catch erlang:integer_to_list(I, 16) of
        {'EXIT', _} ->
            old_integer_to_hex(I);
        Int ->
            Int
    end.

old_integer_to_hex(I) when I<10 ->
    integer_to_list(I);
old_integer_to_hex(I) when I<16 ->
    [I-10+$A];
old_integer_to_hex(I) when I>=16 ->
    N = trunc(I/16),
    old_integer_to_hex(N) ++ old_integer_to_hex(I rem 16).

string_to_int(S) ->
    case string:to_integer(S) of 
      {Int,[]} -> Int;
      {error,no_integer} -> null
    end.


compose_body(Args) ->
    lists:concat(
        lists:foldl(
            fun (Rec, []) -> [Rec]; (Rec, Ac) -> [Rec, "&" | Ac] end,
            [],
            [atol(K) ++ "=" ++ twitter_client_utils:url_encode(V) || {K, V} <- Args]
        )
    ).

%% This lets us use atoms as keys for compose_body.

atol(Str) when is_atom(Str) ->
    atom_to_list(Str);
atol(Str) ->
    Str.

%% Takes a status record and returns a tuple suitable for handing off
%% to mochijson2:encode

status_to_json(Status) ->
    {struct,
     [
      {created_at, Status#status.created_at},
      {id, Status#status.id},
      {text, Status#status.text},
      {source, Status#status.source},
      {truncated, Status#status.truncated},
      {in_reply_to_status_id, Status#status.in_reply_to_status_id},
      {in_reply_to_user_id, Status#status.in_reply_to_user_id},
      {favorited, Status#status.favorited},
      {user, user_to_json(Status#status.user)}]}.

%% Takes a user record and returns a tuple suitable for handing off to
%% mochijson2:encode

user_to_json(User) ->
    {struct,
     [
      {id, User#user.id},
      {name, User#user.name},
      {screen_name, User#user.screen_name},
      {location, User#user.location},
      {description, User#user.description},
      {profile_image_url, User#user.profile_image_url},
      {url, User#user.url},
      {protected, User#user.protected},
      {followers_count, User#user.followers_count},
      {status, User#user.status},
      {profile_background_color, User#user.profile_background_color},
      {profile_text_color, User#user.profile_text_color},
      {profile_link_color, User#user.profile_link_color},
      {profile_sidebar_fill_color, User#user.profile_sidebar_fill_color},
      {profile_sidebar_border_color, User#user.profile_sidebar_border_color},
      {friends_count, User#user.friends_count},
      {created_at, User#user.created_at},
      {favourites_count, User#user.favourites_count},
      {utc_offset, User#user.utc_offset},
      {time_zone, User#user.time_zone},
      {following, User#user.following},
      {notifications, User#user.notifications},
      {statuses_count, User#user.statuses_count}]}.
