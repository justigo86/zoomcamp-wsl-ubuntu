# Simple file to print included argument value as desired day of month
# then create a simple 2x2 pandas dataframe and print it
# then save the dataframe to a parquet file

## To build Docker image
docker build -t [image_name]:[img_ver_tag] .

## To run Docker container
docker run -it --rm [image_name]:[img_ver_tag] [int_argument_required]