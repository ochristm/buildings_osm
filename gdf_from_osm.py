#############################
import parse_osm
#############################
print()
print("___________________________")
print("Part 2. Read osm file and save layers to gdfs_shps.")
print("Please wait...")
print()
import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely
import shapely.wkt
from shapely.geometry import Point, LineString, MultiLineString, Polygon
from tqdm.notebook import tqdm

import wget
from osgeo import gdal, ogr
import os
import momepy
from datetime import datetime
import re
import girs
from girs.feat.layers import LayersReader
import os.path
import overpass

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(conda_dir, 'Library\share')
# proj_lib = os.path.join(os.path.join(conda_dir, 'pkgs'), 'proj4-5.2.0-h6538335_1006\Library\share')
path_gdal = os.path.join(proj_lib, 'gdal')
os.environ ['PROJ_LIB']=proj_lib
os.environ ['GDAL_DATA']=path_gdal

# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'
#############################

# filename = './data/map_2_Yaroslavl,Russia_20200430_2004.osm'
#filename = ''
filename = parse_osm.filename
poly_osmid = parse_osm.poly_osmid
while (os.path.isfile(filename) != True):
    print("Введите путь и название файла OSM, пример:")
    print("./data/map_2_Yaroslavl,Russia_20200430_2004.osm")
    filename = input()
    if os.path.isfile(filename) == False:
        print("Файл не найден, попробуйте снова")
    else:
        print("Файл найден,", filename)
# 

list_split = filename[:-4].rsplit("_")
str_date = list_split[-2] + "_" + list_split[-1]
place = list_split[-3]
buff_km = list_split[-4]

# str_date = '20200430_2004'
# place = 'Yaroslavl,Russia'
# buff_km = '2'

#gdf_poly = parse_osm.gdf_poly
#buff_km = parse_osm.buff_km
#############################
path_data = '.\\data\\' + str(poly_osmid) + '\\' + str_date
path_raw = path_data + '\\raw'
path_res = path_data + '\\res'
path_raw_csv = path_raw
#path_raw_shp = path_raw + '\\shp'
path_raw_shp_layers = path_raw + '\\layers'
path_raw_shp_poly = path_raw_shp_layers

#############################
#gdf_poly = ox.gdf_from_place(place, which_result=2, buffer_dist=int(buff_km)*1000)
#gdf_poly.crs='epsg:4326'
#gdf_poly = parse_osm.gdf_poly
#############################

# https://github.com/OSGeo/gdal/blob/master/gdal/data/osmconf.ini
# перед запуском сохрани в папку с этим скриптом файл osmconf.ini из ссылки выше

lrs = LayersReader(filename)

def GirsGdf(lr_nm):
    new_df = lrs.get_geometries_and_field_values(layer_number=lr_nm, geometry_format='wkt')
    lst_geo=[]
    new_df = new_df.reset_index()
    del new_df['FID']
    
    for i in (range(len(new_df))):
        one_geo = new_df._GEOM_[i]
        lst_geo.append(shapely.wkt.loads(one_geo))
    #
    try:
        new_df['geometry'] = lst_geo
        del new_df['_GEOM_']
        new_gdf = gpd.GeoDataFrame(new_df)
        new_gdf.crs='epsg:4326'
    except:
        pass
    del new_df
    return new_gdf
#
try:
    gdf_lines = GirsGdf(1)
    gdf_points = GirsGdf(0)
    gdf_multilines = GirsGdf(2)
    gdf_multipolygons = GirsGdf(3)
except(RuntimeError):
    print("RuntimeError, data is too big")
    exit()
#gdf_other = GirsGdf(4)
del lrs

#extract polygon of the city from all polygons
gdf_poly = gdf_multipolygons[gdf_multipolygons.osm_id==poly_osmid][['osm_id', 'name', 
                                                              'place', 'other_tags', 'geometry']].reset_index(drop=True)
#
if len(gdf_poly) > 0:
    gdf_poly.crs='epsg:4326'
    try:
    #gdf_poly = gdf_poly.iloc[[0]]
        gdf_poly.geometry[0] = gdf_poly.geometry[0][0]
    except:
        pass
#

else:
    try:
        api = overpass.API()
        poly_resp = api.get(
            """[out:json][timeout:25];
            relation({});
            (._;>;);
            out geom;""".format(poly_osmid), 
            build=False, responseformat="json")
        # Collect coords into list
        coords = []
        for element in poly_resp['elements']:
            if element['type'] == 'way':
                one_line = []
                for j in range(len(element['geometry'])):
                    lon = element['geometry'][j]['lon']
                    lat = element['geometry'][j]['lat']
                    one_line.append((lon, lat))
                coords.append(LineString(one_line))
        #
        ml = MultiLineString(coords)
        buff_ml = ml.buffer(0.000000003)
        pl = ml.convex_hull
        diff = pl.difference(buff_ml)
        lst_len = []
        i=0
        max_ind = -1
        for i in range(len(diff)):
            len_poly = diff[i].length
            lst_len.append(len_poly)
            if len_poly == max(lst_len):
                max_ind = i
        # 
        boundary = diff[max_ind]
        gdf_poly=gpd.GeoDataFrame(geometry=[boundary])
        gdf_poly['osm_id'] = poly_osmid
        gdf_poly.crs='epsg:4326'
    except:
        pass
#
if int(buff_km) > 0:
    buffer = int(buff_km) * 1000
    try:
        gdf_poly.geometry = gdf_poly.geometry.to_crs('epsg:32637').buffer(buffer).to_crs('epsg:4326')
    except:
        pass
#

gdf_lines = gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway', 
                           'barrier', 'man_made', 'other_tags', 'geometry']]
                           
# создание поля z_order, алгоритм на sql написан в файле osmconf.ini
# это поле показывает порядок (уровень) ребер/улиц (важно на многоуровневых эстакадах)

# при импорте скрипта для чтения с ОСМ - колонка z_order генерится сама
# без импорта - ниже функция создания этого поля
def CreateZorderColumn(gdf_lines):
    reg = re.compile('[^0-9-]') # only ints
    dict_z_order = {}
    i=0
    for i in range(len(gdf_lines)):
        osmid = gdf_lines.osm_id[i]
        dict_z_order[osmid] = 0
        if gdf_lines.highway[i] in ['minor','road','unclassified','residential']:
            dict_z_order[osmid] = 3
        elif gdf_lines.highway[i] in ['tertiary_link','tertiary']:
            dict_z_order[osmid] = 4
        elif gdf_lines.highway[i] in ['secondary_link','secondary']:
            dict_z_order[osmid] = 6
        elif gdf_lines.highway[i] in ['primary_link','primary']:
            dict_z_order[osmid] = 7
        elif gdf_lines.highway[i] in ['trunk_link','trunk']:
            dict_z_order[osmid] = 8
        elif gdf_lines.highway[i] in ['motorway_link','motorway']:
            dict_z_order[osmid] = 9
        else:
            dict_z_order[osmid] = 0
        #
        if (('"bridge"=>"yes"' in str(gdf_lines['other_tags'][i]))
        or
        ('"bridge"=>"true"' in str(gdf_lines['other_tags'][i]))
        or
        ('"bridge"=>"1"' in str(gdf_lines['other_tags'][i]))):
            dict_z_order[osmid] = dict_z_order[osmid] + 10
        #
        if (('"tunnel"=>"yes"' in str(gdf_lines['other_tags'][i]))
        or
        ('"tunnel"=>"true"' in str(gdf_lines['other_tags'][i]))
        or
        ('"tunnel"=>"1"' in str(gdf_lines['other_tags'][i]))):
            dict_z_order[osmid] = dict_z_order[osmid] - 10
        #
        if ('railway' in str(gdf_lines['other_tags'][i])):
            dict_z_order[osmid] = dict_z_order[osmid] + 5
        #
        if ('layer' in str(gdf_lines['other_tags'][i])):

            str1 = gdf_lines.other_tags[i]
            str2 = str1[str1.find('"layer"=>"') : ].split(",", 1)[0]
            int_layer = int(reg.sub('', str2))
            dict_z_order[osmid] = dict_z_order[osmid] + (10*int_layer)
        #
    # 
    df_dict = pd.DataFrame(columns=['osm_id', 'z_order'])
    df_dict['osm_id'] = list(dict_z_order.keys())
    df_dict['z_order'] = list(dict_z_order.values())
    # если значения будут расходиться, можно проверить с изначальным графом
    # read_shp = gpd.read_file(r'./for_exp/lines_yaroslavl.shp', encoding = 'utf-8')

    # res_compare = read_shp[['osm_id', 'z_order']] == df_dict[['osm_id', 'z_order']]
    # res_compare[res_compare.z_order == False]
    
    gdf_lines = gdf_lines.merge(df_dict, on=['osm_id'], how='left')
    gdf_lines = gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway', 
                           'barrier', 'man_made', 'z_order', 'other_tags', 'geometry']].reset_index(drop=True)
    return gdf_lines
#
gdf_lines = CreateZorderColumn(gdf_lines)


######################

gdf_buildings = gdf_multipolygons[~gdf_multipolygons.building.isna()].reset_index(drop=True)
gdf_buildings = gpd.sjoin(gdf_buildings, gdf_poly[['geometry']], how='inner', op='intersects')
gdf_buildings = gdf_buildings.drop("index_right", axis=1).reset_index(drop=True)

###############################
# saving shp

gdf_lines[['osm_id', 'name', 'highway', 'waterway', 'aerialway',
           'barrier', 'man_made', 'z_order', 
           'other_tags']].to_csv("{}\\csv_{}_{}_{}.csv".format(path_raw_csv,buff_km, place, str_date), sep=";", encoding="utf-8-sig", index=False)
#
# сохранение без геометрии - нужны только поля.
# если кодировка просто utf-8, то сохраняются каракули вместо букв, 
# а windows-1251 или cp1251 не декодируют какие-то конкретные символы, я хз почему

# обрезать для сохранения в шейп
gdf_lines_shp = gdf_lines.copy()
i=0

for i in range(len(gdf_lines_shp)):
    if len(str(gdf_lines_shp.other_tags[i])) > 254:
        gdf_lines_shp.other_tags[i] = gdf_lines_shp.other_tags[i][:254]
#


gdf_buildings_shp = gdf_buildings.copy()
i=0

for i in range(len(gdf_buildings_shp)):
    if len(str(gdf_buildings_shp.other_tags[i])) > 254:
        gdf_buildings_shp.other_tags[i] = gdf_buildings_shp.other_tags[i][:254]
#

gdf_lines_shp.to_file("{}\\gdf_lines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_points.to_file("{}\\gdf_points_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_multilines.to_file("{}\\gdf_multilines_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_multipolygons.to_file("{}\\gdf_multipolygons_{}_{}_{}.shp".format(path_raw_shp_layers,buff_km, place, str_date), encoding="utf-8")
gdf_buildings_shp.to_file("{}\\gdf_buildings_{}_{}_{}.shp".format(path_res,buff_km, place, str_date), encoding="utf-8")

try:
    gdf_poly.to_file('{}\\poly_{}_{}_{}.shp'.format(path_raw_shp_poly,buff_km, place, str_date), encoding='utf-8')
except:
    pass
    print("Borders of the region are not saved to shp")
print("All raw data is saved to folder 'raw'")
#
print("Done")
