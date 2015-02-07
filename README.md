impact
==========

Provide realtime shaking intensities for impact using seedlink data

Configuration
-----------------

The input sites json file provides a lookup for each expected stream, the information expected will be:
a hash with the key being the stream name, i.e. *<NN>_<SSS>_<LL>_<CCC>* with the following expected fields,
any missing fields will be set to zero or have an empty string.

 * Longitude
 * Latitude
 * Q
 * Rate
 * Gain
 * Name

Parameters
------------

The routines need AWS parameters, stream configuration, and noise settings.
