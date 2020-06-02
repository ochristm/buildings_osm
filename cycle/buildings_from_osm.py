import os
import os.path
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(conda_dir, 'Library\share')
# proj_lib = os.path.join(os.path.join(conda_dir, 'pkgs'), 'proj4-5.2.0-h6538335_1006\Library\share')
path_gdal = os.path.join(proj_lib, 'gdal')
os.environ ['PROJ_LIB']=proj_lib
os.environ ['GDAL_DATA']=path_gdal

import networkx as nx
import osmnx as ox
import pandas as pd
import geopandas as gpd
import shapely

import wget
import gdal, ogr

import momepy
from datetime import datetime
import re
import girs
from girs.feat.layers import LayersReader
import overpass
from pyproj import Proj, transform
########

import shapely.wkt
from shapely.geometry import Point, LineString, MultiLineString, Polygon
import wget
###

# # В случе ошибки RuntimeError: b'no arguments in initialization list'
# # Если действие выше не помогло, то нужно задать системной переменной PROJ_LIB
# # явный путь к окружению по аналогии ниже
# Для настройки проекции координат, поменять на свой вариант


# os.environ ['PROJ_LIB']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share'
# os.environ ['GDAL_DATA']=r'C:\Users\popova_kv\AppData\Local\Continuum\anaconda3\Library\share\gdal'

#отключить предупреждения pandas (так быстрее считает!!!):
pd.options.mode.chained_assignment = None

import warnings
warnings.filterwarnings("ignore")

###############################################

##########################
def parse_osm_data(rel_id):

	api = overpass.API()

	resp = api.get(
			"""[out:json][timeout:25];
			relation({});
			out bb;""".format(rel_id), 
			build=False, responseformat="json")
	#
	dict_bbox = resp["elements"][0]["bounds"]
	poly_osmid = str(rel_id)

	#########################

	str_date = "{:%Y%m%d_%H%M}".format(datetime.now())

	path_new = os.getcwd()
	path_total = path_new + '\\data'
	path_city = path_total + '\\' + str(poly_osmid)
	path_data = path_city + '\\' + str_date
	path_raw = path_data + '\\raw'
	path_raw_osm = path_raw
	path_raw_csv = path_raw
	#path_raw_shp = path_raw + '\\shp'
	path_raw_shp_layers = path_raw
	path_raw_shp_poly = path_raw_shp_layers

	path_res = path_data + '\\res'
	path_res_edges = path_res
	path_res_nodes = path_res

	list_paths = [path_total, path_city, path_data, path_raw, path_raw_shp_layers, path_res]

	for path in list_paths:
		try:
			os.mkdir(path)
		except FileExistsError:
			pass
		except OSError:
			print ("Не удалось создать директорию: %s \n" % path)
		# else:
			# print ("Создана директория %s \n" % path)
	# 

	#########################

	buffer = 0
	buff_km = 0

	#################################

	new_minlat, new_minlon = dict_bbox['minlat'], dict_bbox['minlon']
	new_maxlat, new_maxlon = dict_bbox['maxlat'], dict_bbox['maxlon']

	#################################

	# вот так выглядит ссылка для скачивания вручную:
	# https://overpass-api.de/api/map?bbox=37.321,56.517,41.232,58.978
	#это для определения, какие координаты за какую сторону света отвечают в ссылке
	# west,south,east,north = 37.321,56.517,41.232,58.978 

	url_new = str("https://overpass-api.de/api/map?bbox="
				  +str(new_minlon)+","
				  +str(new_minlat)+","
				  +str(new_maxlon)+","
				  +str(new_maxlat))
	#
	# с помощью wget можно скачивать большие объемы, например, области
	filename = wget.download(url_new, out='{}\\map_{}_{}.osm'.format(path_raw_osm, rel_id, str_date), bar=None)
	return filename
#


def get_layer(filename):
	list_split = filename[:-4].rsplit("_")
	str_date = list_split[-2] + "_" + list_split[-1]
	rel_id = list_split[-3]
	poly_osmid = rel_id
	buff_km = 0
	###
	path_new = os.getcwd()
	path_data = path_new + '\\data\\' + str(poly_osmid) + '\\' + str_date
	path_res = path_data + '\\res'
	###
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

	######################

	gdf_buildings = gdf_multipolygons[~gdf_multipolygons.building.isna()].reset_index(drop=True)
	gdf_buildings = gpd.sjoin(gdf_buildings, gdf_poly[['geometry']], how='inner', op='intersects')
	gdf_buildings = gdf_buildings.drop("index_right", axis=1).reset_index(drop=True)

	###############################

	# сохранение без геометрии - нужны только поля.
	# если кодировка просто utf-8, то сохраняются каракули вместо букв, 
	# а windows-1251 или cp1251 не декодируют какие-то конкретные символы, я хз почему

	# обрезать для сохранения в шейп
	gdf_buildings_shp = gdf_buildings.copy()
	i=0

	for i in range(len(gdf_buildings_shp)):
		if len(str(gdf_buildings_shp.other_tags[i])) > 254:
			gdf_buildings_shp.other_tags[i] = gdf_buildings_shp.other_tags[i][:254]
	#

	gdf_buildings_shp.to_file("{}\\gdf_buildings_{}_{}.shp".format(path_res, rel_id, str_date), encoding="utf-8")

	try:
		gdf_poly.to_file('{}\\poly_{}_{}.shp'.format(path_res, rel_id, str_date), encoding='utf-8')
	except:
		pass
		print("Borders of the region are not saved to shp")
	#

#################################
def get_data(rel_id):
	filename = parse_osm_data(rel_id)
	get_layer(filename)

######################
