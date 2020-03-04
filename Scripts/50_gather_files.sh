cd Datos/
awk '
    FNR==1 && NR!=1 { while (/^ORIGIN_AIRPORT_ID/) getline; }
    1 {print}
' *.csv > all.csv
