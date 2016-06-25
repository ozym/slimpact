impact
==========

Provide shaking intensities for impact via go

Theory
------------

Real-time seedlink derived miniseed blocks, or those recovered from files, are passed through
a set of first order filters to convert the signal to a velocity stream. The actual filters
used depends on whether the input signal is velocity or acceleration. These simply require
a channel gain (in units of counts/m/sec or counts/m/sec^2) and a high-pass filter parameter _q_.

[Continuous Monitoring of Ground-Motion Parameters by Hiroo Kanamori, Philip Maechling, and Egill Hauksson](http://authors.library.caltech.edu/37034/1/311.full.pdf)

The derived velocites are converted to an integer MMI estimate based on the Italian model of Faenza & Michelini:

  5.11 + 2.35 * log(100.0 * vel)

[Regression analysis of MCS Intensity and ground motion parameters in Italy and its application in ShakeMap (2009) by L. Faenza and A. Michelini](http://www.earth-prints.org/handle/2122/5302)

To reduce the impact of noisy channels, a simple noise detection scheme is employed. Configuration is based around a probation time and a noise threshold.
If the signal is above the noise level continuously for the probation time it will be noted as _noisy_ and will no longer produce messages. The stream
then needs to be below the noise level continuously for the same probation time before it will be considered as no longer _noisy_.

Results
--------------

The integer intensities are sent via a simple JSON encoded message to an AWS SQS queue. The following fields are used:

 * source
 * quality
 * latitude
 * longitude
 * time
 * MMI
 * comment

