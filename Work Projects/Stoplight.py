# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import folium as fm

Pt_claims = pd.read_csv(r"insert file name", sep = '\t')

Pt_geoaddr = pd.read_csv(r"insert file name")

# import the provider data
Pro_addr = pd.read_csv(r"insert file name", sep='\t')

# merge the queries together to pull lat and long into the patient addresses
Pt_merge = pd.merge(Pt_claims, Pt_geoaddr, how = 'left', left_on = ['ADDR1', 'CITY', 'ST'], right_on = ['Address', 'City', 'State'])

# drop the extra address columns for clarity
Pt_merge.drop(columns=['Address', 'City', 'State', 'Zip'], inplace = True)

# drop addresses for those who did not match for lat and long
Pt_merge.dropna(axis=0, how='any', subset=['lat', 'lng'], inplace = True)
Pro_addr.dropna(axis=0, how='any', subset=['lat', 'lng'], inplace = True)

# need to change the data type from float to int to display correctly
Pro_addr['UNDWTR'] = Pro_addr['UNDWTR'].astype(int)

BCBS_Spec=['Allergy Immunology', 'Cardiology', 'Dermatology', 'Endocrinology', 'Gastroenterology', 'General Surgery', 'Infectious Disease', 'Nephrology', 'Neurology', 'Neurosurgery', 'Oncology Medical', 'Radiation Oncology', 'Ophthalmology', 'Orthopedic Surgery', 'Otolaryngology', 'Plastic and Reconstructive Surgery', 'Podiatry', 'Psychiatry', 'Pulmonary Disease', 'Rheumatology', 'Thoracic Surgery', 'Urology', 'Vascular Surgery', 'Family Practice', 'Obstetrics Gynecology', 'Pediatrics']

for specialty in BCBS_Spec:
    
    # create the base map for the Chicago metro area
    Chi_map = fm.Map([41.8, -87.7], tiles='CartoDB dark_matter', zoom_start=9)
    
    # iterate over the rows in the Pro_merge data frame
    for row in Pro_addr.itertuples():
        # map only the providers for the target specialty
        if row.DESCRIPTION == specialty:
            # map only the in network providers
            if row.NETWORK == 'I':
                if row.UNDWTR == 488:
                    # create blue flags for the 488 network providers
                    fm.Marker(location=[row.lat, row.lng],
                          popup=fm.Popup(html=row.PROV_OFFIC + '\n' + '(' + str(row.UNDWTR) + ')', parse_html=True),
                          icon=fm.Icon(color='blue', icon=None)).add_to(Chi_map)
                else:
                    # create orange flags for the 197 network providers
                    fm.Marker(location=[row.lat, row.lng],
                          popup=fm.Popup(html=row.PROV_OFFIC + '\n' + '(' + str(row.UNDWTR) + ')', parse_html=True),
                          icon=fm.Icon(color='orange', icon=None)).add_to(Chi_map)
                    
    # create names for the feature groups, which allows us to add selectable layers to the map
    in_network = fm.FeatureGroup(name='In Network')
    consulting = fm.FeatureGroup(name='Consulting') 
    out_of_network = fm.FeatureGroup(name='Out of Network')
             
    for row in Pt_merge.itertuples():
        if row.DESCRIPTION == specialty:
            if row.NETWORK == 'I':
                # create green circles for the in-network patient claims
                fm.Circle(location=[row.lat, row.lng], 
                                radius=(400), 
                                color='#00000000',
                                fill=True,
                                fill_color='#008000',
                                fill_opacity=0.3,
                                popup = 'Claims: ' + str(row.CLAIMS)).add_to(in_network)
            elif row.NETWORK == 'C':
                # create yellow circles for the consulting patient claims
                fm.Circle(location=[row.lat, row.lng], 
                                radius=(400),
                                color='#00000000',
                                fill=True,
                                fill_color='#FFFF00',
                                fill_opacity=0.6,
                                popup = 'Claims: ' + str(row.CLAIMS)).add_to(consulting)
            else:
                # create red circels for the out of network patient claims
                fm.Circle(location=[row.lat, row.lng], 
                                radius=(400),
                                color='#00000000',
                                fill=True,
                                fill_color='#FF0000',
                                fill_opacity=0.3,
                                popup = 'Claims: ' + str(row.CLAIMS)).add_to(out_of_network)
    
    # plot all of the data to the base map
    in_network.add_to(Chi_map)
    consulting.add_to(Chi_map)
    out_of_network.add_to(Chi_map)
    fm.LayerControl(collapsed=False).add_to(Chi_map)
    
    # save the map as an aspx file for publishing to Sharepoint (consider adding a date stamp to the filename for future)
    Chi_map.save("AMITA " + specialty + " map.aspx")
    
