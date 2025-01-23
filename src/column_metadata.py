column_mapping = {
    'station': 'str',     # Station identifier
    'time': 'str',        # Timestamp
    'wsi': 'str',         # Weather station identifier
    'stationname': 'str', # Station name
    'lat': 'Float64',     # Latitude
    'lon': 'Float64',     # Longitude
    'height': 'Float64',  # Height
    'D1H': 'Float64',     # 1-hour direction
    'dd': 'Float64',      # Wind direction
    'dn': 'Float64',      # Wind direction minimum
    'dr': 'Float64',      # Wind direction range
    'dsd': 'Float64',     # Wind direction standard deviation
    'dx': 'Float64',      # Wind direction maximum
    'ff': 'Float64',      # Wind speed
    'ffs': 'Float64',     # Wind speed sensor
    'fsd': 'Float64',     # Wind speed standard deviation
    'fx': 'Float64',      # Wind gust
    'fxs': 'Float64',     # Wind gust sensor
    'gff': 'Float64',     # Wind speed ground
    'gffs': 'Float64',    # Wind speed ground sensor
    'h': 'Float64',       # Cloud base height
    'h1': 'Float64',      # Low cloud height
    'h2': 'Float64',      # Medium cloud height
    'h3': 'Float64',      # High cloud height
    'hc': 'Float64',      # Cloud base height corrected
    'hc1': 'Float64',     # Low cloud base height corrected 
    'hc2': 'Float64',     # Medium cloud base height corrected
    'hc3': 'Float64',     # High cloud base height corrected
    'n': 'Float64',       # Total cloud cover
    'n1': 'Float64',      # Low cloud cover
    'n2': 'Float64',      # Medium cloud cover
    'n3': 'Float64',      # High cloud cover
    'nc': 'Float64',      # Total cloud cover corrected
    'nc1': 'Float64',     # Low cloud cover corrected
    'nc2': 'Float64',     # Medium cloud cover corrected
    'nc3': 'Float64',     # High cloud cover corrected
    'p0': 'Float64',      # Sea level pressure
    'pp': 'Float64',      # Pressure tendency
    'pg': 'Float64',      # Ground pressure
    'pr': 'Float64',      # Precipitation rate
    'ps': 'Float64',      # Surface pressure
    'pwc': 'Float64',     # Present weather code corrected
    'Q1H': 'Float64',     # 1-hour radiation
    'Q24H': 'Float64',    # 24-hour radiation
    'qg': 'Float64',      # Global radiation
    'qgn': 'Float64',     # Global radiation minimum
    'qgx': 'Float64',     # Global radiation maximum
    'qnh': 'Float64',     # QNH pressure
    'R12H': 'Float64',    # 12-hour precipitation
    'R1H': 'Float64',     # 1-hour precipitation
    'R24H': 'Float64',    # 24-hour precipitation
    'R6H': 'Float64',     # 6-hour precipitation
    'rg': 'Float64',      # Precipitation amount
    'rh': 'Float64',      # Relative humidity
    'rh10': 'Float64',    # 10m relative humidity
    'Sav1H': 'Float64',   # 1-hour sunshine average
    'Sax1H': 'Float64',   # 1-hour sunshine maximum
    'Sax3H': 'Float64',   # 3-hour sunshine maximum
    'Sax6H': 'Float64',   # 6-hour sunshine maximum
    'sq': 'Float64',      # Sunshine duration
    'ss': 'Float64',      # Snow depth
    'Sx1H': 'Float64',    # 1-hour solar radiation
    'Sx3H': 'Float64',    # 3-hour solar radiation
    'Sx6H': 'Float64',    # 6-hour solar radiation
    't10': 'Float64',     # 10m temperature
    'ta': 'Float64',      # Air temperature
    'tb': 'Float64',      # Soil temperature
    'tb1': 'Float64',     # Soil temperature level 1
    'Tb1n6': 'Float64',   # Soil temperature level 1 min 6h
    'Tb1x6': 'Float64',   # Soil temperature level 1 max 6h
    'tb2': 'Float64',     # Soil temperature level 2
    'Tb2n6': 'Float64',   # Soil temperature level 2 min 6h
    'Tb2x6': 'Float64',   # Soil temperature level 2 max 6h
    'tb3': 'Float64',     # Soil temperature level 3
    'tb4': 'Float64',     # Soil temperature level 4
    'tb5': 'Float64',     # Soil temperature level 5
    'td': 'Float64',      # Dewpoint temperature
    'td10': 'Float64',    # 10m dewpoint temperature
    'tg': 'Float64',      # Ground temperature
    'tgn': 'Float64',     # Ground temperature minimum
    'Tgn12': 'Float64',   # Ground temperature minimum 12h
    'Tgn14': 'Float64',   # Ground temperature minimum 14h
    'Tgn6': 'Float64',    # Ground temperature minimum 6h
    'tn': 'Float64',      # Temperature minimum
    'Tn12': 'Float64',    # Temperature minimum 12h
    'Tn14': 'Float64',    # Temperature minimum 14h
    'Tn6': 'Float64',     # Temperature minimum 6h
    'tsd': 'Float64',     # Temperature standard deviation
    'tx': 'Float64',      # Temperature maximum
    'Tx12': 'Float64',    # Temperature maximum 12h
    'Tx24': 'Float64',    # Temperature maximum 24h
    'Tx6': 'Float64',     # Temperature maximum 6h
    'vv': 'Float64',      # Visibility
    'W10': 'Float64',     # Past weather 1
    'W10-10': 'Float64',  # Past weather 2
    'ww': 'Float64',      # Present weather
    'ww-10': 'Float64',   # Past present weather
    'zm': 'Float64',      # Model height
    'iso_dataset': 'str', # Dataset identifier
    'product': 'str',     # Product type
    'projection': 'str',  # Map projection
    'nhc': 'str',         # Non-hydrostatic correction
    'za': 'str'           # Zone identifier
}

column_renaming = {
    'station': 'station',
    'time': 'time',
    'wsi': 'weather_station_id',
    'stationname': 'station_name',
    'lat': 'latitude',
    'lon': 'longitude',
    'height': 'height',
    'D1H': 'direction_1h',
    'dd': 'wind_direction',
    'dn': 'wind_direction_min',
    'dr': 'wind_direction_range',
    'dsd': 'wind_direction_std',
    'dx': 'wind_direction_max',
    'ff': 'wind_speed',
    'ffs': 'wind_speed_sensor',
    'fsd': 'wind_speed_std',
    'fx': 'wind_gust',
    'fxs': 'wind_gust_sensor',
    'gff': 'wind_speed_ground',
    'gffs': 'wind_speed_ground_sensor',
    'h': 'cloud_base_height',
    'h1': 'low_cloud_height',
    'h2': 'medium_cloud_height',
    'h3': 'high_cloud_height',
    'hc': 'cloud_base_height_corrected',
    'hc1': 'low_cloud_height_corrected',
    'hc2': 'medium_cloud_height_corrected',
    'hc3': 'high_cloud_height_corrected',
    'n': 'total_cloud_cover',
    'n1': 'low_cloud_cover',
    'n2': 'medium_cloud_cover',
    'n3': 'high_cloud_cover',
    'nc': 'total_cloud_cover_corrected',
    'nc1': 'low_cloud_cover_corrected',
    'nc2': 'medium_cloud_cover_corrected',
    'nc3': 'high_cloud_cover_corrected',
    'p0': 'sea_level_pressure',
    'pp': 'pressure_tendency',
    'pg': 'ground_pressure',
    'pr': 'precipitation_rate',
    'ps': 'surface_pressure',
    'pwc': 'present_weather_corrected',
    'Q1H': 'radiation_1h',
    'Q24H': 'radiation_24h',
    'qg': 'global_radiation',
    'qgn': 'global_radiation_min',
    'qgx': 'global_radiation_max',
    'qnh': 'qnh_pressure',
    'R12H': 'precipitation_12h',
    'R1H': 'precipitation_1h',
    'R24H': 'precipitation_24h',
    'R6H': 'precipitation_6h',
    'rg': 'precipitation_amount',
    'rh': 'relative_humidity',
    'rh10': 'relative_humidity_10m',
    'Sav1H': 'sunshine_avg_1h',
    'Sax1H': 'sunshine_max_1h',
    'Sax3H': 'sunshine_max_3h',
    'Sax6H': 'sunshine_max_6h',
    'sq': 'sunshine_duration',
    'ss': 'snow_depth',
    'Sx1H': 'solar_radiation_1h',
    'Sx3H': 'solar_radiation_3h',
    'Sx6H': 'solar_radiation_6h',
    't10': 'temperature_10m',
    'ta': 'air_temperature',
    'tb': 'soil_temp',
    'tb1': 'soil_temp_level1',
    'Tb1n6': 'soil_temp_level1_min_6h',
    'Tb1x6': 'soil_temp_level1_max_6h',
    'tb2': 'soil_temp_level2',
    'Tb2n6': 'soil_temp_level2_min_6h',
    'Tb2x6': 'soil_temp_level2_max_6h',
    'tb3': 'soil_temp_level3',
    'tb4': 'soil_temp_level4',
    'tb5': 'soil_temp_level5',
    'td': 'dewpoint_temp',
    'td10': 'dewpoint_temp_10m',
    'tg': 'ground_temp',
    'tgn': 'ground_temp_min',
    'Tgn12': 'ground_temp_min_12h',
    'Tgn14': 'ground_temp_min_14h',
    'Tgn6': 'ground_temp_min_6h',
    'tn': 'temp_min',
    'Tn12': 'temp_min_12h',
    'Tn14': 'temp_min_14h',
    'Tn6': 'temp_min_6h',
    'tsd': 'temp_std',
    'tx': 'temp_max',
    'Tx12': 'temp_max_12h',
    'Tx24': 'temp_max_24h',
    'Tx6': 'temp_max_6h',
    'vv': 'visibility',
    'W10': 'past_weather_1',
    'W10-10': 'past_weather_2',
    'ww': 'present_weather',
    'ww-10': 'past_present_weather',
    'zm': 'model_height',
    'iso_dataset': 'dataset_id',
    'product': 'product_type',
    'projection': 'map_projection',
    'nhc': 'non_hydrostatic_correction',
    'za': 'zone_id'
}