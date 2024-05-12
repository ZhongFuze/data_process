#!/bin/sh
curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0x99C19AB10b9EC8aC6fcda9586E81f6B73a298870
# Return Value: {"domainName":"test123.gno"}Time: 4.470421s

curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0x2886D6792503e04b19640C1f1430d23219AF177F
# {"domainName":null} Time: 2.569239s
curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0xFCEeC24912535A47C0Cba436977537AD225A2562
# {"domainName":"genome.gno"} Time: 2.727718s

curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0xc9Af7BbEa391364e40FD6eBdE0e9911129d94905
# {"domainName":"gnome.gno"} Time: 2.935862s

curl -w "\nTime: %{time_total}s\n" http://localhost:22222/lookup/gno/0x6D910bEa79aAf318E7170C6fb8318D9c466B2164
# {"domainName":"shiva.gno"} Time: 2.764978s