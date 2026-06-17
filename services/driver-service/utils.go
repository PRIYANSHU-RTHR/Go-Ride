package main

import "math/rand"

// Predefined routes for drivers in Gandhinagar, Gujarat
var PredefinedRoutes = [][][]float64{
	// Route 1: Akshardham → Sector 16 → Mahatma Mandir
	{
		{23.2303, 72.6469},
		{23.2315, 72.6488},
		{23.2330, 72.6510},
		{23.2348, 72.6534},
		{23.2367, 72.6560},
	},

	// Route 2: Infocity Circle → GIFT Road
	{
		{23.1947, 72.6365},
		{23.1963, 72.6390},
		{23.1985, 72.6421},
		{23.2012, 72.6455},
		{23.2038, 72.6488},
		{23.2065, 72.6519},
	},

	// Route 3: IIT Gandhinagar → PDPU Road
	{
		{23.2158, 72.6842},
		{23.2143, 72.6809},
		{23.2126, 72.6765},
		{23.2107, 72.6718},
		{23.2086, 72.6672},
		{23.2068, 72.6624},
	},

	// Route 4: Sector 21 → Akshardham → Infocity
	{
		{23.2236, 72.6578},
		{23.2261, 72.6542},
		{23.2284, 72.6507},
		{23.2303, 72.6469},
		{23.2265, 72.6412},
		{23.2207, 72.6375},
		{23.2149, 72.6360},
	},

	// Route 5: Mahatma Mandir → Gift City Connector
	{
		{23.2367, 72.6560},
		{23.2351, 72.6590},
		{23.2332, 72.6622},
		{23.2309, 72.6655},
		{23.2278, 72.6690},
		{23.2245, 72.6728},
	},
}

func GenerateRandomPlate() string {
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	plate := ""
	for i := 0; i < 3; i++ {
		plate += string(letters[rand.Intn(len(letters))])
	}

	return plate
}
