# buildings_osm

Порядок действий:
1. Прочитать и выполнить требования из readme.txt
2. Открыть консоль Anaconda.Prompt
3. В консоли ввести тот путь, где лежит этот скрипт (перейти туда)
3.1 Должно быть написано что-то типа такого:
C:\Users\user\...\buildings_osm>
4. Ввести: python run.py
5. Следовать инструкциям на экране


__________________________________________________
ТРЕБОВАНИЯ:
1. 1 В файле gdf_from_osm.py proj_lib поменять на свой
2. В папке со скриптом должен находиться файл osmconf.ifi, 
отсюда: # https://github.com/OSGeo/gdal/blob/master/gdal/data/osmconf.ini
4. Должны быть установлены следующие модули:

* datetime
* gdal
* geopandas
* girs
* momepy
* networkx
* numpy
* ogr
* os
* osgeo
* osmnx
* overpass
* pandas
* pyproj
* re
* shapely
* tqdm
* wget



(proj4 и rasterio устанавливать через):
* conda install -c conda-forge proj4
* conda install -c conda-forge rasterio
