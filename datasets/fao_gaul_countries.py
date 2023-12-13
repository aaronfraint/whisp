import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# from datasets.template_images import template_image_1

# dataset_id = 16

ee.Initialize()

def fao_gaul_countries_prep(dataset_id,template_image):
    
    import modules.area_stats as area_stats
    
    from datasets.reproject_to_template import reproject_to_template
    
    
    # path_lookup_country_codes_to_names = "parameters/lookup_GAUL_country_codes_to_names.csv"

    GAUL_boundaries_poly = ee.FeatureCollection("FAO/GAUL/2015/level0");

    GAUL_code_column = "ADM0_CODE"

    template = ee.Image("UMD/hansen/global_forest_change_2022_v1_10");

    GAUL_boundaries_adm0_code = GAUL_boundaries_poly.reduceToImage([GAUL_code_column],ee.Reducer.mode())  #make into image with the admn0 country code as the value

    #reproject based on template (tyically gfc data - approx 30m res)
    GAUL_boundaries_adm0_code_reproj = reproject_to_template(GAUL_boundaries_adm0_code,template_image)

    GAUL_boundaries_adm0_code_reproj = area_stats.set_scale_property_from_image(
        GAUL_boundaries_adm0_code_reproj,template_image,0,debug=True)

    output_image = GAUL_boundaries_adm0_code_reproj
    
    return output_image.set("dataset_id",dataset_id)