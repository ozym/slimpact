package impact

import (
	"math"
)

//
// Wald, Quitoriano, Heaton, and Kanimori (Earthquake Spectra, Volume 15, No. 3, August 1999).
// would be 2.35 + 3.47*math.Log10(100.0 * vel)
//

// Regression analysis of MCS Intensity and ground motion parameters in Italy and its application
// in ShakeMap (2009) by L. Faenza and A. Michelini

// convert peak velocity in m/s into intensity
func RawIntensity(vel float64) float64 {
	return 5.11 + 2.35*math.Log10(100.0*vel)
}

// convert peak velocity in m/s into integer intensity
func Intensity(vel float64) int32 {
	if vel <= 0.0 {
		return 1
	}
	raw := RawIntensity(vel)
	if raw <= 1.0 {
		return 1
	}
	if raw >= 12.0 {
		return 12
	}
	return (int32)(math.Floor(raw))
}
