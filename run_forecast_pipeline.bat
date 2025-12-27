@echo off
setlocal

echo [1/3] Get GFS forecast subset...
python src/get_gfs_forecast.py --fxx 24 --north 37 --south 25 --west -107 --east -93 || exit /b 1

echo [2/3] Compute forecast anomalies...
python src/compute_forecast_anomalies.py || exit /b 1

echo [3/3] Make anomaly map...
python src/make_anomaly_map.py --anoms_nc data/processed/forecast_anoms.nc --var t2m_anom_c --out reports/figures/anom_t2m.png || exit /b 1

echo Done.
endlocal
