# Sumner-Tunnel-VMS-Prediction-Accuracy-Validation
Simple script designed to grab predicted departure and arrival times using the MBTA's V3-API for specific stations as a result of the Sumner Tunnel Closure. \
Routes include: (Wonderland -> State), (Beachmont -> State), (Newburyport -> North Station), (Beverly -> North Station).\

### Departure time scripts
`predictions_3` selects predictions if available/schedules if not.\
`predictions_4` inserts both into the database table, `None` if no predictions exist.

### Comparison script
`comp_logic_2` produces a `.txt` file containing the inconsistencies found between `vmslog` and the MBTA's `V3-API`.\
In particular, the comparison logic highlights entries in `vmslog` where scheduled departures were displayed instead of predicted departures.\
Currently, `comp_logic-2` also produces a second `.txt` file which contains any message errors that have been displayed on VMS signs.
