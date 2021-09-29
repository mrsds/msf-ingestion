import rasterio
import rasterio.features
import rasterio.warp
import os
import gdal
import json


def get_geojson_from_tiff(tiff_file, verbose=False):

    geom = None
    
        
    with rasterio.open(tiff_file) as dataset:

        # Read the dataset's valid data mask as a ndarray.
        mask = dataset.dataset_mask()

        # Extract feature shapes and values from the array.
        try:
            for geom, val in rasterio.features.shapes(
                    mask, transform=dataset.transform):

                # Transform shapes from the dataset's own coordinate
                # reference system to CRS84 (EPSG:4326).
                geom = rasterio.warp.transform_geom(
                    dataset.crs, 'EPSG:4326', geom, precision=6)
        except Exception as e:
            with rasterio.Env(OGR_ENABLE_PARTIAL_REPROJECTION=True):
                try:
                    for geom, val in rasterio.features.shapes(
                        mask, transform=dataset.transform):

                        # Transform shapes from the dataset's own coordinate
                        # reference system to CRS84 (EPSG:4326).
                        geom = rasterio.warp.transform_geom(
                            dataset.crs, 'EPSG:4326', geom, precision=6)
                except Exception as e1:
                    print(str(e1))
                    raise e1
 

    return geom



def convert_files(input, out_dir, verbose=False):

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    input_file_list = []

    if os.path.isdir(input):
        for root, dirs, files in os.walk(input):
            input_file_list = [os.path.join(input, filename) for filename in files if filename.endswith("_rgb.tif")]
    elif os.path.isfile(input):
        with open(input) as f:
            input_file_list  = f.readlines()
            input_file_list = [x.strip() for x in input_file_list] 

    for filename in input_file_list:
        base_filename = filename.replace("_rgb.tif", "")
        print("Processing : {}".format(filename))
        try:
            geom = get_geojson_from_tiff(filename, verbose)
       
            if geom:
                out_file = os.path.join(out_dir, "{}_geojson.json".format((os.path.basename(base_filename))))
                with open(out_file, 'w') as outfile:
                    json.dump(geom, outfile)
                    if verbose:
                        print("wrote to file {} : {}".format(out_file, geom))
            else:
                print("Failed to process : {}".format(filename))
        except Exception as e:
            filename = "{}_ctr.tif".format(base_filename)
            print("Processing : {} as rgb failed".format(filename))
            try:
                geom = get_geojson_from_tiff(filename, verbose)
                if geom:
                    with open(out_file, 'w') as outfile:
                        json.dump(geom, outfile)
                        if verbose:
                            print("wrote to file {} : {}".format(out_file, geom))
                else:
                    print("Failed to process : {}".format(filename))
            except Exception as e2:
                 print("Failed to process {} : {}".format(filename, str(e2)))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Input tif file directory or filename containing all the tif file name with path", required=True)
    parser.add_argument("-o", "--outdir", help="Output geojson directory", type=str, default="geojson_data", required=False)
    parser.add_argument("-v", "--verbose",
                        help="Extra output",
                        required=False, action="store_true")

    args = parser.parse_args()
    convert_files(args.input, args.outdir, args.verbose)
