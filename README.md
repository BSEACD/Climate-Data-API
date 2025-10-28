# Climate-Data-API
A Python-based API was developed to streamline the process of downloading, processing, and analyzing PRISM Climate Group datasets. An API acts as a standardized gateway that enables different software applications to communicate and exchange data. This methodology is particularly useful for handling and processing large, complex, spatiotemporal datasets, as the interface allows users to request specific data from a server without needing to understand or engage with the underlying computational complexity of the server's internal systems. Python was selected as the programming language for this API due to its extensive libraries and capacity for seamless application integration. The custom tool features a defined workflow designed to automate the entire data processing pipeline from initial request to final output. 

Upon receiving a user request, the API executes a series of actions, including: 

-  Establishing a connection between the user’s request and the PRISM data source. 

-  Executing a sequence of processing actions:
  - Download climate data time-series directly from the PRISM server. 
  - Automate the extraction of data files from zipped folders.  
  - Organize and sort data files into a logical folder structure. 
  - Clip the downloaded rasters to a user defined geographic area of interest (AOI).  
  - Calculate key statistics from the processed rasters. 
  - Save all calculations to a CSV spreadsheet. 
  
-  Return the requested dataset to the user in a clean, processed format suitable for immediate analysis and visualization. 

Currently, the API is operational for downloading PRISM precipitation data in millimeters or inches, at both monthly and daily resolutions. 

Python libraries required for external use:
  - datetime
  - requests
  - numpy
  - rasterio
  - rasterio.mask
  - os
  - shutil
  - traceback
  - logging
  - zipfile
  - pandas
  - geopandas
  - time
  - csv
  - shapely
  - sys
  - regex
  - dateutil.relativedelta
