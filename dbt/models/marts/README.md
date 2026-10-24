Add the final dimensional models here after source semantics and the validated event schema are confirmed.
Recommended initial grain: one row per unique meter reading for FactMeterReadings; one row per transformer per reporting date for FactDailyDemand.
Do not create FactMeterReadings keyed only on `id` until we confirm whether `id` uniquely identifies a reading or a meter.
