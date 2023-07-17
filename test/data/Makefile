
# Generate benchmark data from a osm.pbf file

NAME = germany

$(NAME).o5m: $(NAME).osm.pbf
	osmconvert $< -o=$(NAME).o5m

# Roads
$(NAME)/roads/layer.o5m: $(NAME).o5m
		mkdir -p $(dir $@)
		osmfilter $< -o=$@ \
		--keep="highway=motorway =motorway_link =trunk =trunk_link =primary =primary_link =secondary =secondary_link =tertiary =tertiary_link =service =unclassified =residential" --keep-tags="all id= type= highway= tunnel= bridge=" \
		--modify-tags="bridge=* to =yes tunnel=* to =yes highway= to kind="

$(NAME)/roads/layer.geojsonseq: $(NAME)/roads/layer.o5m
	osmium export -c config_roads.json --geometry-types=linestring -f geojsonseq $< -o $@

$(NAME)/roads/roads.fgb: $(NAME)/roads/layer.geojsonseq
	ogr2ogr -f FlatGeobuf $@ $<


# Forests
$(NAME)/forest/layer.o5m: $(NAME).o5m
		mkdir -p $(dir $@)
		osmfilter $< -o=$@ --keep="landuse=forest or natural=wood" --keep-tags="all id= type= landuse= natural="

$(NAME)/forest/layer.geojsonseq: $(NAME)/forest/layer.o5m
		osmium export -c config_forest.json --keep-untagged --geometry-types=polygon -f geojsonseq $< > $@

$(NAME)/forest/forest.fgb: $(NAME)/forest/layer.geojsonseq
	# explode collections so we only have polygons
	ogr2ogr -f FlatGeobuf $@ $< -explodecollections