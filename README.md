# Sumner-Tunnel-VMS-Prediction-Accuracy-Validation
Simple script designed to grab predicted departure and arrival times using the MBTA's V3-API for specific stations as a result of the Sumner Tunnel Closure. \
Routes include: (Wonderland -> State), (Beachmont -> State), (Newburyport -> North Station), (Beverly -> North Station).\

### Departure time scripts
`predictions_3` selects predictions if available/schedules if not.\
`predictions_4` inserts both into the database table, `None` if no predictions exist.

### Comparison script
`comp_logic` produces a `.txt` file containing the inconsistencies found between `vmslog` and the MBTA's `V3-API`.
