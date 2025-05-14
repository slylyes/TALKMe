
#! /bin/bash

curl --noproxy localhost --location 'http://localhost:8080/api/table' --header 'Content-Type: application/json' --data '{"name": "Table","columns": {"surcharge": {"values": [],"type": "DOUBLE"},"Total_Amt": {"values": [],"type": "DOUBLE"},"Trip_Dropoff_DateTime": {"values": [],"type": "BINARY"},"Start_Lon": {"values": [],"type": "DOUBLE"},"Fare_Amt": {"values": [],"type": "DOUBLE"},"Tolls_Amt": {"values": [],"type": "DOUBLE"},"Rate_Code": {"values": [],"type": "DOUBLE"},"vendor_name": {"values": [],"type": "BINARY"},"Tip_Amt": {"values": [],"type": "DOUBLE"},"End_Lat": {"values": [],"type": "DOUBLE"},"Payment_Type": {"values": [],"type": "BINARY"},"Start_Lat": {"values": [],"type": "DOUBLE"},"store_and_forward": {"values": [],"type": "DOUBLE"},"Trip_Distance": {"values": [],"type": "DOUBLE"},"End_Lon": {"values": [],"type": "DOUBLE"},"Passenger_Count": {"values": [],"type": "INT64"},"mta_tax": {"values": [],"type": "DOUBLE"},"Trip_Pickup_DateTime": {"values": [],"type": "BINARY"}}}'

echo ' '
