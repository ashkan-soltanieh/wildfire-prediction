import cdsapi

c = cdsapi.Client()

variables = {1:'2m_temperature',2:'10m_v_component_of_wind', 3:'10m_u_component_of_wind', 4:'convective_available_potential_energy',
            5:'convective_inhibition', 6:'2m_dewpoint_temperature', 7:'total_precipitation', 8:'total_cloud_cover',
            9:'high_vegetation_cover',10:'low_vegetation_cover', 11:'volumetric_soil_water_layer_1'}

for i in range(1,len(variables) + 1):
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': variables[i],
            'year': [
                '2010', '2011', '2012',
                '2013', '2014', '2015',
                '2016', '2017', '2018',
                '2019'
            ],
            'month': [
                '01', '02', '03', '04',
                '05', '06', '07',
                '08', '09', '10', '11', '12'
            ],
            'day': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
                '13', '14', '15',
                '16', '17', '18',
                '19', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '30',
                '31',
            ],
            'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
            ],
            'area': [
                60, -131.5, 47.75,
                -110,
            ], #'area': [60, -134.25,48.5,-110] # ideal area to cover all AB and BC fires
        },
        'data' + '-' + variables[i] + '.nc')