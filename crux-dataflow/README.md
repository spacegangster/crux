# Crux Dataflow Spike

*SPIKE / pre-ALPHA*

Integrates Crux with https://github.com/sixthnormal/clj-3df and
https://github.com/comnik/declarative-dataflow

## What it is
Declarative Dataflow is a reactive data environment that not only computes
queries but then computes and broadcasts the differential updates to those queries.

Itself it's built on top of
https://github.com/timelydataflow/timely-dataflow
https://github.com/comnik/declarative-dataflow/tree/master/docs/adr

See paper "Naiad: a timely dataflow system"
 https://dl.acm.org/citation.cfm?id=2522738

## Build and run
Build and run the declarative-dataflow v0.2.0 server directly from
git:

```
git clone https://github.com/comnik/declarative-dataflow/tree/v0.2.0
git checkout v0.2.0
cd server
cargo build
cargo run
```

There's a version of this binary living at
`resources/declarative-server-v0.2.0-x86_64-unknown-linux-gnu` as
well. Note that the server really has v0.1.0, but the
declarative-dataflow version is v0.2.0.


## Differential dataflow server
Schema is cleared every time it restarts
from clj-3df side it's taking ~450ms for a round-trip
between `exec!` and query-listener code.

But only 1500ms for a batch of size 500
```
  (listen-query! conn "loans>50" :timer
    (fn [msg]
      (let [elapsed-time (- (System/currentTimeMillis) @time-start)]
        (swap! end-times conj elapsed-time)
        (prn "Msg" msg
          (str "Elapsed time: " elapsed-time " msecs")))))

  (reset! time-start (System/currentTimeMillis))
  (exec! conn
    (transact db [{:db/id        23
                    :loan/amount  2000
                    :loan/from    "B"
                    :loan/to      "A"
                    :loan/over-50 true}]))

  (reset! time-start (System/currentTimeMillis))
  (dotimes [i 500]
    (exec! conn
      (transact db [{:db/id        (+ i 7)
                     :loan/amount  2000
                     :loan/from    "B"
                     :loan/to      "A"
                     :loan/over-50 true}])))
```

## TODO
- [*] investigate the indexing bug with tx-log
- [*] split subscription key and query key
- [*] 3DF string/uuid ids
- [*] transact in only required query data
- [*] transact in full results data, so better query modification
- [*] query data shape - vector
- [*] polling failure resiliency

- [ ] query data shape - map
- [ ] automatic query keys

- [ ] collection values ser / deser

- [ ] ingest transact in entities bindings from rules
- [ ] evict/delete/cas txs

optional
- [ ] manage external 3df server processes?

## Architecture

## Known Caveats
For 