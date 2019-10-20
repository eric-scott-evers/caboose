
-module(caboose).
-compile([export_all]).
% -export([start/0]).

% caboose is demand driven data flow
%   processes A, B, C
%   requests for data flow from C to B to A
%   data flows from A to B to C

start() ->
  display_intro(),
  Engine = spawn(?MODULE, engine, []),
  A = spawn(?MODULE, build_car, [a]),
  B = spawn(?MODULE, build_car, [b]),
  C = spawn(?MODULE, build_car, [c]),
  A ! {tank, is, [1,2,3,4,5,6,7,8,9,10,11,12]},
  F = fun(X) -> X * 2 end,
  A ! {function, is, F}, % function is not applied in caboose
  B ! {function, is, F},
  C ! {function, is, F},
  C ! {request, is, 4},
  Engine ! [A,B,C].

engine() ->
  Bun = dict:from_list([
    {name, engine},
    {cars,[]}
  ]),
  engine(Bun).

% Engine receives a list of Pids of cars
%   init the cars
%   watch the data of the last car
%     is the output of the train

engine(Bun) ->
  Name = dict:fetch(name, Bun),
  receive
    L when is_list(L) ->
      NBun = dict:store(cars, L, Bun),
      rpc:call(node(), io, fwrite,
         ["~w ~n", [{list, is, L, in, Name}]]),
      init_cars(NBun),
      lists:last(L) ! {watch, is, true},
      hd(L) ! {label, is, caboose},
      hd(L) ! {engine, is, self()},
      link_all(L),
      engine(NBun);
    kill_all ->
      exit(ok)
  end,
  engine(Bun).

link_all([]) -> ok;
link_all([H|T])->
  link(H),
  link_all(T).

% init_cars where cars learn about their bosses and sources
%   the boss is on the right
%   the source is on the left
%   [A,B,C]

init_cars(Bun) ->
  Cars = dict:fetch(cars,Bun),
  tell_cars_boss(Cars),
  tell_cars_source(lists:reverse(Cars)).

tell_cars_boss([A,B|T]) when length(T) >= 0 ->
  A ! {boss, is, B},
  tell_cars_boss([B|T]);
tell_cars_boss(_) ->
  ok.

tell_cars_source([A,B|T]) when length(T) >= 0 ->
  A ! {source, is, B},
  tell_cars_source([B|T]);
tell_cars_source(_) ->
  ok.

% Each car has
%   a tank: list for inputs
%   and
%   a data: list for outputs
%

% Build a car with initial values
%
build_car(Name) ->
  Bun = dict:from_list([
    {watch, none},
    {name,Name},
    {label,car},
    {tank,[]},
    {data,[]},
    {source,none},
    {boss,none},
    {request,none},
    {function,none},
    {engine,none}
  ]),
  car(Bun).

car(Bun) ->
  Name = dict:fetch(name,Bun),
  compute(Bun),
  flow_request(Bun),
  pump_request(Bun),
  watch(Bun),
  am_i_done(Bun),
  receive
    {Key, is, Value} ->
      NBun = dict:store(Key,Value,Bun),
      rpc:call(node(), io, fwrite,
         ["~w ~n", [{Key, is, Value, in, Name}]]),
      car(NBun)
  end,
  car(Bun).

am_i_done(Bun) ->
  Label  = dict:fetch(label,Bun),
  Tank   = dict:fetch(tank, Bun),
  Data   = dict:fetch(data, Bun),
  Engine = dict:fetch(engine, Bun),
  if (Label == caboose) and (Tank == []) and (Data == []) ->
    bomb(Engine);
  true -> ok
  end.

% kill all processes
%
bomb(Engine) ->
  receive
    after 1000 ->
      Engine ! kill_all
  end.

watch(Bun) ->
  Name = dict:fetch(name,Bun),
  Watch = dict:fetch(watch,Bun),
  Data = dict:fetch(data,Bun),
  if Watch == true ->
    rpc:call(node(), io, fwrite,
      ["~w ~n", [{output, data, is, Data, in, Name}]]);
  true -> ok
  end.

% compute
%   before compute, Tank contains raw info, Data is empty
%   after  compute, Tank is empty, Data = f(Tank)
compute(Bun) ->
    Tank = dict:fetch(tank, Bun),
    Data = dict:fetch(data, Bun),
    Function = dict:fetch(function, Bun),
    if not (Function==none) ->
      if not (Tank==[]) ->
        NData = lists:map(Function, Tank),
        N1Bun = dict:store(tank,[],Bun),
        N2Bun = dict:store(data,NData++Data,N1Bun),
        car(N2Bun);
      true -> ok
      end;
    true -> ok
    end.

% flow_request
%   if Data is empty the request data from source

flow_request(Bun) ->
  Data = dict:fetch(data, Bun),
  Source = dict:fetch(source, Bun),
  Request = dict:fetch(request, Bun),
  if length(Data) == 0 ->
    if not (Request==none) and not (Source==none) ->
      Source ! {request, is, Request};
    true ->
      ok
    end;
    true -> ok
  end.

pump_request(Bun) ->
    Data = dict:fetch(data, Bun),
    Boss = dict:fetch(boss, Bun),
    Request = dict:fetch(request, Bun),
    if length(Data) > 0 ->
      if not (Request==none) and not (Boss==none) ->
        Boss ! {tank, is, Data},
        N1Bun = dict:store(data,[],Bun),
        N2Bun = dict:store(request,none,N1Bun),
        car(N2Bun);
      true ->
        ok
      end;
      true -> ok
    end.

display_intro() ->
  io:format(
  " Caboose is a demand driven chain of processes. ~n" ++
  " a <- b <- c ~n" ++
  " a -> c -> c ~n" ++
  " a is a caboose, b is a car, c is an engine ~n" ++
  " requests for data flow from engine to caboose ~n" ++
  " data flows from caboose to engine ~n" ++
  " all a, b and c have a tank that holds outputs ~n" ++
  " all a, b and c have a data list that hold inputs "
  " all a, b and c have a function ~n" ++
  " but the function is not applied in a caboose ~n" ++
  " the function turns inputs into outputs ~n"
  , []),
  ok.
