

SECTORS = {
    "1 Energy": {
        "1A Fuel Combustion Activities": [
            "1A1 Energy Industries",
            "1A2 Manufacturing Industries & Construction",
            "1A3 Transport",
            "1A4 Other Sectors",
            "1A5 Non-Specified"
        ],
        "1B Fugitive Emissions from Fuels": [
            "1B1 Solid Fuels",
            "1B2 Oil & Natural Gas",
            "1B2 Oil and Natural Gas",
            "1B3 Other Emissions from Energy Production"
        ],
        "1C Carbon Dioxide Transport & Storage": [
            "1C1 Transport of CO2",
            "1C2 Injections & Storage",
            "1C3 Other"
        ]
    },
    "2 Industrial Processes & Product Use": {

        "2A Mineral Industrya*": [],
        "2B Chemical Industry*": [],
        "2C Metal Industry*": [],
        "2D Non-Energy Products from Fuels & Solvent Use*": [],
        "2E Electronics Industry*": [],
        "2F Product Uses as Substitues for Ozone Depletion*": [], # sic
        "2G Other Product Manufacture & Use*": [],
        "2H Other*": []
    },
    "3 Agriculture, Forestry & Other Land Use": {
        "3A Livestock": [
            "3A1 Enteric Fermentation",
            "3A2 Manure Management",
            "3A1 & 3A2 Enteric Fermentation and Manure Management"
        ],
        "3B Land": [
            "3B1 Forest Land",
            "3B2 Cropland",
            "3B3 Grassland",
            "3B4 Wetlands",
            "3B5 Settlements",
            "3B6 Other Land"
        ],
        "3C Aggregate Sources & Non-CO2 Emissions*": [],
        "3D Other*": []
    },
    "4 Waste": {
        "4A Solid Waste Disposal": [
            "4A1 Managed Waste Disposal Sites",
            "4A2 Unmanaged Waste Disposal Sites",
            "4A3 Uncategorised Waste Disposal Sites"
        ],
        "4B Biological Treatment of Solid Waste": [
            "4B1 Biological Treatment of Solid Waste"
        ],
        "4C Incineration & Open Burning of Waste*": [],
        "4D Wastewater Treatment & Discharge": [
            "4D1 Domestic Wastewater Treatment & Discharge",
            "4D2 Industrial Wastewater Treatment & Discharge",
            "4D1 & 4D2 Domestic Wastewater Treatment and Discharge"
        ],
        "4E Other*": []
    },
    "5 Other": {
        "5A Indirect N2O Emissions from the Atmospheric Deposition of Nitrogen Nox and NH3*": [],
        "5B Other*": []
    }

}


LEVEL_3_LIST = []


for level_1_name in SECTORS:
    level_1_map = SECTORS[level_1_name]
    for level_2_name in level_1_map:
        level_2_list = level_1_map[level_2_name]
        for level_3_name in level_2_list:
            LEVEL_3_LIST.append([level_3_name, level_2_name, level_1_name])


def get_sectors_by_level_3(l3_name):
    for l3 in LEVEL_3_LIST:
        if l3[0].lower() == l3_name.lower():
            return l3
    return None, None, None