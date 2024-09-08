import numpy as np

def round_to_nearest_quarter(value):

	# Round value to the nearest quarter
	rounded_value = np.round(value * 4) / 4
	# Get the decimal part
	decimal_part = rounded_value % 1
	
	# Adjust to ensure the value ends with 0.25 or 0.75
	if decimal_part in [0.25, 0.75]:
		return rounded_value
	else:
		# Find the nearest 0.25 or 0.75
		lower_bound = np.floor(rounded_value) + 0.25
		upper_bound = np.floor(rounded_value) + 0.75
		if abs(rounded_value - lower_bound) < abs(rounded_value - upper_bound):
			return lower_bound
		else:
			return upper_bound
		


def find_closest(lat, lon, df):
    # Calculate the absolute differences
    df['lat_diff'] = np.abs(df['Lat'] - lat)
    df['lon_diff'] = np.abs(df['Lon'] - lon)
    
    # Find the row with the minimum sum of differences
    min_diff_index = df[['lat_diff', 'lon_diff']].sum(axis=1).idxmin()
    
    # Return the row with the minimum difference
    return df.loc[min_diff_index]



def get_description_from_cls(cls_value, location_data_1):

    descriptions = {category: {} for category in location_data_1['Category'].unique()}
    
    for _, row in location_data_1.iterrows():
        category = row['Category']
        code = row['Code']
        description = row['Description']
        if code not in descriptions[category]:
            descriptions[category][code] = description
    
    description_results = {}
    for category, code_desc_map in descriptions.items():
        for code in cls_value:
            if code in code_desc_map:
                description_results[category] = code_desc_map[code]
                break
    
    return description_results