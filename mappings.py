# Comprehensive Flight Parser Mappings (Airports, Airlines, and Timezones)
# Updated with all major commercial airports worldwide

AIRPORT_CODES = {
    # ===== INDIA (Major hubs first) =====
    "DEL": "Delhi", "BOM": "Mumbai", "BLR": "Bengaluru", "MAA": "Chennai", "CCU": "Kolkata",
    "HYD": "Hyderabad", "AMD": "Ahmedabad", "PNQ": "Pune", "COK": "Kochi", "GOI": "Goa",
    "JAI": "Jaipur", "TRV": "Thiruvananthapuram", "GAU": "Guwahati", "LKO": "Lucknow",
    "NAG": "Nagpur", "IXC": "Chandigarh", "VNS": "Varanasi", "PAT": "Patna", "BBI": "Bhubaneswar",
    "IXB": "Bagdogra", "IXR": "Ranchi", "IDR": "Indore", "RPR": "Raipur", "VGA": "Vijayawada",
    "IXE": "Mangalore", "IXM": "Madurai", "IXU": "Aurangabad", "SXR": "Srinagar", "IXZ": "Port Blair",
    "IMF": "Imphal", "DIB": "Dibrugarh", "JRH": "Jorhat", "IXJ": "Jammu", "ATQ": "Amritsar",
    "IXL": "Leh", "UDR": "Udaipur", "BDQ": "Vadodara", "RAJ": "Rajkot", "STV": "Surat",
    "PBD": "Porbandar", "BHJ": "Bhuj", "BHO": "Bhopal", "JLR": "Jabalpur", "GWL": "Gwalior",
    "AGR": "Agra", "IXD": "Allahabad", "VDY": "Vadodara", "RJA": "Rajahmundry", "TIR": "Tirupati",
    "BEP": "Bellary", "HBX": "Hubli", "IXG": "Belgaum", "GOP": "Gorakhpur", "DED": "Dehradun",
    "PGH": "Pantnagar", "TNI": "Satna", "KUU": "Kullu Manali", "SHL": "Shillong", "IXS": "Silchar",
    "AJL": "Aizawl", "IXA": "Agartala", "DMU": "Dimapur", "CBD": "Car Nicobar", "IXV": "Along",
    
    # ===== UNITED STATES (Major hubs first) =====
    "ATL": "Atlanta Hartsfield-Jackson", "LAX": "Los Angeles", "ORD": "Chicago O'Hare", 
    "DFW": "Dallas Fort Worth", "DEN": "Denver", "JFK": "New York JFK", "SFO": "San Francisco",
    "LAS": "Las Vegas", "SEA": "Seattle-Tacoma", "MCO": "Orlando", "EWR": "Newark Liberty",
    "CLT": "Charlotte Douglas", "PHX": "Phoenix Sky Harbor", "IAH": "Houston George Bush",
    "MIA": "Miami", "BOS": "Boston Logan", "MSP": "Minneapolis-St Paul", "DTW": "Detroit Metro",
    "FLL": "Fort Lauderdale", "PHL": "Philadelphia", "LGA": "New York LaGuardia", 
    "BWI": "Baltimore/Washington", "IAD": "Washington Dulles", "DCA": "Washington Reagan",
    "MDW": "Chicago Midway", "SAN": "San Diego", "TPA": "Tampa", "PDX": "Portland",
    "HNL": "Honolulu", "STL": "St Louis", "BNA": "Nashville", "AUS": "Austin-Bergstrom",
    "HOU": "Houston Hobby", "OAK": "Oakland", "MSY": "New Orleans", "SJC": "San Jose",
    "RDU": "Raleigh-Durham", "SLC": "Salt Lake City", "SAT": "San Antonio", "RSW": "Fort Myers",
    "PIT": "Pittsburgh", "CVG": "Cincinnati", "CMH": "Columbus", "IND": "Indianapolis",
    "MCI": "Kansas City", "CLE": "Cleveland", "SNA": "Santa Ana", "SMF": "Sacramento",
    "MKE": "Milwaukee", "JAX": "Jacksonville", "ONT": "Ontario CA", "BUR": "Burbank",
    "RNO": "Reno-Tahoe", "ABQ": "Albuquerque", "BUF": "Buffalo", "OMA": "Omaha",
    "ANC": "Anchorage", "TUS": "Tucson", "BDL": "Hartford", "PBI": "West Palm Beach",
    "BOI": "Boise", "RIC": "Richmond", "ALB": "Albany", "GRR": "Grand Rapids",
    "DSM": "Des Moines", "LGB": "Long Beach", "SYR": "Syracuse", "ORF": "Norfolk",
    "GEG": "Spokane", "ROC": "Rochester", "ICT": "Wichita", "DAY": "Dayton",
    "PSP": "Palm Springs", "CHS": "Charleston SC", "SAV": "Savannah", "MYR": "Myrtle Beach",
    "FAT": "Fresno", "MAF": "Midland-Odessa", "ELP": "El Paso", "BTV": "Burlington",
    "PWM": "Portland ME", "LEX": "Lexington", "GSP": "Greenville SC", "TYS": "Knoxville",
    "LIT": "Little Rock", "CAK": "Akron-Canton", "SDF": "Louisville", "TUL": "Tulsa",
    
    # ===== EUROPE (Major hubs first) =====
    "LHR": "London Heathrow", "CDG": "Paris Charles de Gaulle", "AMS": "Amsterdam Schiphol",
    "FRA": "Frankfurt", "IST": "Istanbul", "MAD": "Madrid Barajas", "BCN": "Barcelona El Prat",
    "MUC": "Munich", "FCO": "Rome Fiumicino", "LGW": "London Gatwick", "DME": "Moscow Domodedovo",
    "DUB": "Dublin", "ZRH": "Zurich", "CPH": "Copenhagen", "VIE": "Vienna",
    "MXP": "Milan Malpensa", "OSL": "Oslo Gardermoen", "SVO": "Moscow Sheremetyevo",
    "ARN": "Stockholm Arlanda", "BRU": "Brussels", "STN": "London Stansted", "LIS": "Lisbon",
    "ATH": "Athens", "MAN": "Manchester", "OTP": "Bucharest Henri Coanda", "EDI": "Edinburgh",
    "DUS": "Dusseldorf", "LTN": "London Luton", "PRG": "Prague", "BUD": "Budapest",
    "WAW": "Warsaw Chopin", "HAM": "Hamburg", "LCY": "London City", "BER": "Berlin Brandenburg",
    "GVA": "Geneva", "NCE": "Nice Cote d'Azur", "ORY": "Paris Orly", "VCE": "Venice Marco Polo",
    "HEL": "Helsinki-Vantaa", "LIN": "Milan Linate", "BGY": "Bergamo Orio al Serio",
    "KRK": "Krakow", "GLA": "Glasgow", "AGP": "Malaga", "NAP": "Naples", "BHX": "Birmingham UK",
    "STR": "Stuttgart", "BSL": "Basel-Mulhouse", "GOT": "Gothenburg", "BLQ": "Bologna",
    "OPO": "Porto", "BIO": "Bilbao", "CGN": "Cologne Bonn", "PMI": "Palma de Mallorca",
    "LED": "St Petersburg Pulkovo", "TXL": "Berlin Tegel", "VNO": "Vilnius", "RIX": "Riga",
    "TLL": "Tallinn", "SOF": "Sofia", "SKG": "Thessaloniki", "BEG": "Belgrade",
    "ZAG": "Zagreb", "LJU": "Ljubljana", "SPU": "Split", "DBV": "Dubrovnik",
    "SNN": "Shannon", "ORK": "Cork", "BFS": "Belfast", "GDN": "Gdansk", "KTW": "Katowice",
    "WRO": "Wroclaw", "POZ": "Poznan", "CLJ": "Cluj-Napoca", "IAS": "Iasi",
    "BTS": "Bratislava", "MLH": "Mulhouse", "TLS": "Toulouse", "LYS": "Lyon",
    "MRS": "Marseille", "BOD": "Bordeaux", "NTE": "Nantes", "BRE": "Bremen",
    "HAJ": "Hanover", "NUE": "Nuremberg", "LEJ": "Leipzig", "DRS": "Dresden",
    "FMM": "Memmingen", "SCQ": "Santiago de Compostela", "SVQ": "Seville", "VLC": "Valencia",
    "ALC": "Alicante", "IBZ": "Ibiza", "MAH": "Menorca", "TFS": "Tenerife South",
    "LPA": "Gran Canaria", "ACE": "Lanzarote", "FUE": "Fuerteventura", "FAO": "Faro",
    "FNC": "Funchal Madeira", "PDL": "Ponta Delgada", "CAG": "Cagliari", "CTA": "Catania",
    "PMO": "Palermo", "BRI": "Bari", "RHO": "Rhodes", "HER": "Heraklion", "CFU": "Corfu",
    "CHQ": "Chania", "JTR": "Santorini", "ZTH": "Zakynthos", "KGS": "Kos",
    
    # ===== MIDDLE EAST (Major hubs first) =====
    "DXB": "Dubai International", "DOH": "Doha Hamad", "AUH": "Abu Dhabi", "JED": "Jeddah",
    "RUH": "Riyadh King Khalid", "KWI": "Kuwait", "CAI": "Cairo", "TLV": "Tel Aviv Ben Gurion",
    "MCT": "Muscat", "BAH": "Bahrain", "AMM": "Amman Queen Alia", "BEY": "Beirut",
    "SHJ": "Sharjah", "DMM": "Dammam King Fahd", "MED": "Madinah", "TIF": "Taif",
    "GIZ": "Jizan", "AJF": "Al Jouf", "ELQ": "Gassim", "HAS": "Hail", "TUU": "Tabuk",
    "AHB": "Abha", "URY": "Gurayat", "RAE": "Arar", "BGW": "Baghdad", "BSR": "Basra",
    "EBL": "Erbil", "SDA": "Baghdad Al Muthanna", "IKA": "Tehran Imam Khomeini",
    "THR": "Tehran Mehrabad", "MHD": "Mashhad", "SYZ": "Shiraz", "TBZ": "Tabriz",
    "LYX": "Larnaca", "PFO": "Paphos", "ADA": "Adana", "AYT": "Antalya",
    "ESB": "Ankara Esenboga", "SAW": "Istanbul Sabiha Gokcen", "ADB": "Izmir Adnan Menderes",
    "BJV": "Bodrum", "DLM": "Dalaman", "GZT": "Gaziantep", "ASR": "Kayseri",
    
    # ===== EAST ASIA (Major hubs first) =====
    "PEK": "Beijing Capital", "PVG": "Shanghai Pudong", "CAN": "Guangzhou Baiyun",
    "CTU": "Chengdu Shuangliu", "SZX": "Shenzhen Bao'an", "KMG": "Kunming Changshui",
    "XIY": "Xi'an Xianyang", "CKG": "Chongqing Jiangbei", "HGH": "Hangzhou Xiaoshan",
    "NKG": "Nanjing Lukou", "WUH": "Wuhan Tianhe", "SHA": "Shanghai Hongqiao",
    "TSN": "Tianjin Binhai", "TAO": "Qingdao Liuting", "DLC": "Dalian Zhoushuizi",
    "SHE": "Shenyang Taoxian", "CGO": "Zhengzhou Xinzheng", "XMN": "Xiamen Gaoqi",
    "FOC": "Fuzhou Changle", "CSX": "Changsha Huanghua", "NNG": "Nanning Wuxu",
    "KWL": "Guilin Liangjiang", "CGQ": "Changchun Longjia", "HRB": "Harbin Taiping",
    "URC": "Urumqi Diwopu", "LHW": "Lanzhou Zhongchuan", "HET": "Hohhot Baita",
    "HKG": "Hong Kong", "MFM": "Macau", "TPE": "Taipei Taoyuan", "TSA": "Taipei Songshan",
    "KHH": "Kaohsiung", "RMQ": "Taichung", "NRT": "Tokyo Narita", "HND": "Tokyo Haneda",
    "KIX": "Osaka Kansai", "ITM": "Osaka Itami", "NGO": "Nagoya Chubu", "FUK": "Fukuoka",
    "CTS": "Sapporo New Chitose", "OKA": "Okinawa Naha", "KOJ": "Kagoshima", "HIJ": "Hiroshima",
    "ICN": "Seoul Incheon", "GMP": "Seoul Gimpo", "PUS": "Busan Gimhae", "CJU": "Jeju",
    
    # ===== SOUTHEAST ASIA (Major hubs first) =====
    "SIN": "Singapore Changi", "BKK": "Bangkok Suvarnabhumi", "DMK": "Bangkok Don Mueang",
    "CGK": "Jakarta Soekarno-Hatta", "KUL": "Kuala Lumpur", "MNL": "Manila Ninoy Aquino",
    "HAN": "Hanoi Noi Bai", "SGN": "Ho Chi Minh City Tan Son Nhat", "RGN": "Yangon",
    "PNH": "Phnom Penh", "VTE": "Vientiane Wattay", "BWN": "Bandar Seri Begawan",
    "CNX": "Chiang Mai", "HKT": "Phuket", "USM": "Koh Samui", "KBV": "Krabi",
    "HDY": "Hat Yai", "CEI": "Chiang Rai", "UTP": "U-Tapao Pattaya", "URT": "Surat Thani",
    "DPS": "Bali Denpasar", "SUB": "Surabaya", "JOG": "Yogyakarta", "BDO": "Bandung",
    "MDC": "Manado", "UPG": "Makassar", "BPN": "Balikpapan", "PLM": "Palembang",
    "PKU": "Pekanbaru", "BTH": "Batam", "SRG": "Semarang", "SOC": "Solo",
    "PEN": "Penang", "JHB": "Johor Bahru", "KCH": "Kuching", "BKI": "Kota Kinabalu",
    "LGK": "Langkawi", "KUL": "Kuala Lumpur", "SZB": "Kuala Lumpur Subang",
    "CEB": "Cebu", "DVO": "Davao", "ILO": "Iloilo", "CRK": "Clark", "KLO": "Boracay Kalibo",
    "TAC": "Tacloban Daniel Z Romualdez",
    "MPH": "Caticlan", "BCD": "Bacolod", "CBO": "Cotabato", "GES": "General Santos",
    "DAD": "Da Nang", "CXR": "Nha Trang Cam Ranh", "DLI": "Dalat", "VII": "Vinh",
    "HUI": "Hue", "PQC": "Phu Quoc", "VCA": "Can Tho", "BMV": "Buon Ma Thuot",
    "MDL": "Mandalay", "NYU": "Bagan Nyaung U", "HEH": "Heho", "KYP": "Kyaukpyu",
    "REP": "Siem Reap", "KOS": "Sihanoukville", "PNL": "Pantelleria", "LPQ": "Luang Prabang",
    "PKZ": "Pakse", "ZVK": "Savannakhet", "KOP": "Nakhon Phanom",
    
    # ===== SOUTH ASIA (Major hubs first) =====
    "CMB": "Colombo Bandaranaike", "DAC": "Dhaka Hazrat Shahjalal", "KTM": "Kathmandu Tribhuvan",
    "MLE": "Male Velana", "RJH": "Rajshahi", "CXB": "Cox's Bazar", "CGP": "Chittagong",
    "JSR": "Jessore", "ZYL": "Sylhet Osmani", "PKR": "Pokhara", "BWA": "Bhairahawa",
    "BIR": "Biratnagar", "PBH": "Paro", "BUT": "Bathpalathang", "YON": "Yonphula",
    "GLU": "Gelephu", "GAN": "Gan Island", "KDM": "Kaadedhdhoo", "IXW": "Jamshedpur",
    "IXI": "Lilabari", "TEZ": "Tezpur", "RGH": "Balurghat", "COH": "Cooch Behar",
    
    # ===== OCEANIA (Major hubs first) =====
    "SYD": "Sydney Kingsford Smith", "MEL": "Melbourne Tullamarine", "BNE": "Brisbane",
    "PER": "Perth", "AKL": "Auckland", "ADL": "Adelaide", "CNS": "Cairns",
    "OOL": "Gold Coast", "CBR": "Canberra", "HBA": "Hobart", "DRW": "Darwin",
    "WLG": "Wellington", "CHC": "Christchurch", "ZQN": "Queenstown", "DUD": "Dunedin",
    "AVV": "Avalon Melbourne", "MCY": "Sunshine Coast", "BNK": "Ballina Byron",
    "LST": "Launceston", "ASP": "Alice Springs", "TSV": "Townsville", "ROK": "Rockhampton",
    "MKY": "Mackay", "HTI": "Hamilton Island", "PPP": "Proserpine Whitsunday",
    "ABX": "Albury", "OAG": "Orange", "ARM": "Armidale", "TMW": "Tamworth",
    "NSN": "Nelson", "NPL": "New Plymouth", "PMR": "Palmerston North", "TRG": "Tauranga",
    "ROT": "Rotorua", "NPE": "Napier", "BHE": "Blenheim", "IVC": "Invercargill",
    "NOU": "Noumea", "PPT": "Papeete Tahiti", "NAN": "Nadi Fiji", "VLI": "Port Vila",
    "HIR": "Honiara", "APW": "Apia", "POM": "Port Moresby", "GUM": "Guam",
    "SPN": "Saipan", "TRW": "Tarawa", "MAJ": "Majuro", "KWA": "Kwajalein",
    "PNI": "Pohnpei", "TKK": "Chuuk", "ROR": "Koror Palau", "YAP": "Yap",
    
    # ===== CANADA (Major hubs first) =====
    "YYZ": "Toronto Pearson", "YVR": "Vancouver", "YUL": "Montreal Trudeau",
    "YYC": "Calgary", "YEG": "Edmonton", "YOW": "Ottawa", "YWG": "Winnipeg",
    "YHZ": "Halifax", "YQB": "Quebec City", "YYJ": "Victoria", "YXE": "Saskatoon",
    "YQR": "Regina", "YXU": "London ON", "YQM": "Moncton", "YYT": "St John's",
    "YKF": "Kitchener-Waterloo", "YHM": "Hamilton ON", "YQT": "Thunder Bay",
    "YXX": "Abbotsford", "YKA": "Kamloops", "YLW": "Kelowna", "YXS": "Prince George",
    "YZF": "Yellowknife", "YXY": "Whitehorse", "YFB": "Iqaluit", "YQX": "Gander",
    
    # ===== LATIN AMERICA & CARIBBEAN (Major hubs first) =====
    "MEX": "Mexico City", "GRU": "Sao Paulo Guarulhos", "GIG": "Rio de Janeiro Galeao",
    "EZE": "Buenos Aires Ezeiza", "BOG": "Bogota El Dorado", "LIM": "Lima Jorge Chavez",
    "SCL": "Santiago", "CUN": "Cancun", "PTY": "Panama City Tocumen", "UIO": "Quito",
    "GDL": "Guadalajara", "MTY": "Monterrey", "TIJ": "Tijuana", "CUU": "Chihuahua",
    "HMO": "Hermosillo", "BJX": "Leon/Guanajuato", "PVR": "Puerto Vallarta",
    "SJD": "Los Cabos", "MID": "Merida", "CZM": "Cozumel", "ZIH": "Ixtapa-Zihuatanejo",
    "ACA": "Acapulco", "TAP": "Tapachula", "VER": "Veracruz", "OAX": "Oaxaca",
    "CGY": "Cagayan de Oro", "SJO": "San Jose Costa Rica", "SAL": "San Salvador",
    "GUA": "Guatemala City", "TGU": "Tegucigalpa", "MGA": "Managua", "BZE": "Belize City",
    "HAV": "Havana", "PUJ": "Punta Cana", "SDQ": "Santo Domingo", "SJU": "San Juan PR",
    "NAS": "Nassau Bahamas", "KIN": "Kingston Jamaica", "MBJ": "Montego Bay",
    "AUA": "Aruba", "CUR": "Curacao", "BON": "Bonaire", "POS": "Port of Spain",
    "BGI": "Bridgetown Barbados", "GND": "Grenada", "UVF": "St Lucia Hewanorra",
    "SXM": "St Maarten", "SKB": "St Kitts", "ANU": "Antigua", "GCM": "Grand Cayman",
    "SAP": "San Pedro Sula", "RTB": "Roatan", "LIR": "Liberia Costa Rica",
    "BSB": "Brasilia", "CGH": "Sao Paulo Congonhas", "VCP": "Campinas Viracopos",
    "SDU": "Rio de Janeiro Santos Dumont", "CWB": "Curitiba", "POA": "Porto Alegre",
    "FOR": "Fortaleza", "REC": "Recife", "SSA": "Salvador", "BEL": "Belem",
    "MAO": "Manaus", "CGB": "Cuiaba", "GYN": "Goiania", "BHZ": "Belo Horizonte Confins",
    "CNF": "Belo Horizonte Tancredo Neves", "VIX": "Vitoria", "FLN": "Florianopolis",
    "NAT": "Natal", "MCZ": "Maceio", "AJU": "Aracaju", "THE": "Teresina",
    "AEP": "Buenos Aires Aeroparque", "COR": "Cordoba Argentina", "MDZ": "Mendoza",
    "ROS": "Rosario", "BRC": "Bariloche", "USH": "Ushuaia", "IGR": "Iguazu",
    "GYE": "Guayaquil", "CCP": "Concepcion Chile", "ANF": "Antofagasta",
    "IQQ": "Iquique", "LSC": "La Serena", "ZCO": "Temuco", "PUQ": "Punta Arenas",
    "MDE": "Medellin Jose Maria Cordova", "CTG": "Cartagena", "CLO": "Cali",
    "BAQ": "Barranquilla", "BGA": "Bucaramanga", "SMR": "Santa Marta",
    "ADZ": "San Andres", "PEI": "Pereira", "CUC": "Cucuta", "LPB": "La Paz El Alto",
    "VVI": "Santa Cruz Bolivia", "CBB": "Cochabamba", "ASU": "Asuncion",
    "MVD": "Montevideo", "CCS": "Caracas Maiquetia", "VLN": "Valencia Venezuela",
    "MAR": "Maracaibo", "BLA": "Barcelona Venezuela", "PBM": "Paramaribo",
    "CAY": "Cayenne", "GEO": "Georgetown Guyana",
    
    # ===== AFRICA (Major hubs first) =====
    "JNB": "Johannesburg OR Tambo", "CPT": "Cape Town", "CAI": "Cairo", "ADD": "Addis Ababa",
    "LOS": "Lagos Murtala Muhammed", "NBO": "Nairobi Jomo Kenyatta", "CMN": "Casablanca Mohammed V",
    "ALG": "Algiers", "TUN": "Tunis-Carthage", "ACC": "Accra Kotoka", "DKR": "Dakar",
    "DUR": "Durban King Shaka", "BLZ": "Blantyre", "PLZ": "Port Elizabeth",
    "GRJ": "George South Africa", "WDH": "Windhoek Hosea Kutako", "VFA": "Victoria Falls",
    "HRE": "Harare", "LUN": "Lusaka", "DAR": "Dar es Salaam", "ZNZ": "Zanzibar",
    "JRO": "Kilimanjaro", "MBA": "Mombasa", "EBB": "Entebbe", "KGL": "Kigali",
    "BJM": "Bujumbura", "FIH": "Kinshasa", "LBV": "Libreville", "DLA": "Douala",
    "NSI": "Yaounde", "ABJ": "Abidjan", "COO": "Cotonou", "LFW": "Lome",
    "OUA": "Ouagadougou", "ABV": "Abuja", "KAN": "Kano", "PHC": "Port Harcourt",
    "LBV": "Libreville", "FNA": "Freetown", "ROB": "Monrovia", "BJL": "Banjul",
    "DSS": "Dakar Blaise Diagne", "NKC": "Nouakchott", "ASM": "Asmara",
    "JIB": "Djibouti", "MGA": "Managua", "HGA": "Hargeisa", "MGQ": "Mogadishu",
    "MBA": "Mombasa Moi", "TIP": "Tripoli", "MJI": "Mitiga Tripoli",
    "BEN": "Benghazi", "SFA": "Sfax", "DJE": "Djerba", "MIR": "Monastir",
    "RAK": "Marrakech", "FEZ": "Fez", "TNG": "Tangier", "NDR": "Nador",
    "AGA": "Agadir", "OZZ": "Ouarzazate", "ESU": "Essaouira", "ORN": "Oran",
    "CZL": "Constantine", "AAE": "Annaba", "TLM": "Tlemcen", "BSK": "Biskra",
    "SSH": "Sharm el-Sheikh", "HRG": "Hurghada", "LXR": "Luxor", "ASW": "Aswan",
    "MRU": "Mauritius", "RUN": "Reunion", "SEZ": "Seychelles",
    "TNR": "Antananarivo", "NOS": "Nosy Be", "MJN": "Majunga",
    
    # ===== CENTRAL ASIA & OTHERS =====
    "TAS": "Tashkent", "ALA": "Almaty", "NQZ": "Nur-Sultan Astana", "FRU": "Bishkek",
    "DYU": "Dushanbe", "ASB": "Ashgabat", "SKD": "Samarkand", "BHK": "Bukhara",
    "TBS": "Tbilisi", "EVN": "Yerevan", "GYD": "Baku", "KBL": "Kabul",
    "ISB": "Islamabad", "KHI": "Karachi", "LHE": "Lahore", "SKT": "Sialkot",
    "PEW": "Peshawar", "MUX": "Multan", "UET": "Quetta", "ISU": "Sulaymaniyah",
}

AIRLINE_CODES = {
    # ===== INDIA =====
    "6E": "IndiGo", "AI": "Air India", "UK": "Vistara", "QP": "Akasa Air", 
    "SG": "SpiceJet", "IX": "Air India Express", "I5": "AirAsia India", "G8": "GoAir",
    
    # ===== MAJOR GLOBAL CARRIERS =====
    "AA": "American Airlines", "DL": "Delta Air Lines", "UA": "United Airlines",
    "WN": "Southwest Airlines", "AS": "Alaska Airlines", "B6": "JetBlue Airways",
    "NK": "Spirit Airlines", "F9": "Frontier Airlines", "G4": "Allegiant Air",
    
    # ===== EUROPE =====
    "BA": "British Airways", "LH": "Lufthansa", "AF": "Air France", "KL": "KLM Royal Dutch Airlines",
    "IB": "Iberia", "AZ": "ITA Airways", "LX": "SWISS", "OS": "Austrian Airlines",
    "SN": "Brussels Airlines", "SK": "SAS Scandinavian Airlines", "AY": "Finnair",
    "TP": "TAP Air Portugal", "LO": "LOT Polish Airlines", "OK": "Czech Airlines",
    "RO": "Tarom", "JU": "Air Serbia", "OU": "Croatia Airlines", "A3": "Aegean Airlines",
    "FR": "Ryanair", "U2": "easyJet", "W6": "Wizz Air", "VY": "Vueling", "TO": "Transavia",
    "EW": "Eurowings", "DE": "Condor", "X3": "TUI fly", "BY": "TUI Airways",
    "W4": "Wizz Air Malta", "W9": "Wizz Air UK", "MT": "Thomas Cook Airlines",
    "LS": "Jet2.com", "EI": "Aer Lingus", "WX": "CityJet", "BE": "Flybe",
    
    # ===== MIDDLE EAST =====
    "EK": "Emirates", "QR": "Qatar Airways", "EY": "Etihad Airways", "WY": "Oman Air",
    "GF": "Gulf Air", "SV": "Saudia", "MS": "EgyptAir", "RJ": "Royal Jordanian",
    "ME": "Middle East Airlines", "FZ": "flydubai", "G9": "Air Arabia", "XY": "flynas",
    "F3": "Flyadeal", "J9": "Jazeera Airways", "KU": "Kuwait Airways", "5W": "Wizz Air Abu Dhabi",
    
    # ===== ASIA-PACIFIC =====
    "SQ": "Singapore Airlines", "CX": "Cathay Pacific", "TG": "Thai Airways", 
    "MH": "Malaysia Airlines", "GA": "Garuda Indonesia", "PR": "Philippine Airlines",
    "VN": "Vietnam Airlines", "KE": "Korean Air", "OZ": "Asiana Airlines", 
    "JL": "Japan Airlines", "NH": "All Nippon Airways", "CA": "Air China",
    "MU": "China Eastern Airlines", "CZ": "China Southern Airlines", "HU": "Hainan Airlines",
    "CI": "China Airlines", "BR": "EVA Air", "3U": "Sichuan Airlines", "MF": "Xiamen Airlines",
    "HO": "Juneyao Airlines", "FM": "Shanghai Airlines", "SC": "Shandong Airlines",
    "ZH": "Shenzhen Airlines", "GS": "Tianjin Airlines", "8L": "Lucky Air",
    "PN": "West Air", "DR": "Ruili Airlines", "G5": "China Express Airlines",
    
    # ===== LOW-COST ASIA =====
    "AK": "AirAsia", "FD": "Thai AirAsia", "QZ": "Indonesia AirAsia", "Z2": "AirAsia Philippines",
    "D7": "AirAsia X", "I5": "AirAsia India", "TR": "Scoot", "VZ": "Thai Vietjet Air",
    "VJ": "VietJet Air", "PG": "Bangkok Airways", "SL": "Thai Lion Air", "DD": "Nok Air",
    "FD": "Thai AirAsia", "XT": "Indonesia AirAsia X", "OD": "Batik Air Malaysia",
    "ID": "Batik Air", "QG": "Citilink", "JT": "Lion Air", "IU": "Super Air Jet",
    "5J": "Cebu Pacific", "Z2": "Philippines AirAsia", "DG": "Cebgo",
    "BL": "Jetstar Pacific", "VF": "FlyViet", "MJ": "Myway Airlines",
    
    # ===== OCEANIA =====
    "QF": "Qantas", "VA": "Virgin Australia", "JQ": "Jetstar Airways", "NZ": "Air New Zealand",
    "TT": "Tiger Airways Australia", "TE": "FlyPelican", "ZL": "Regional Express",
    "QJ": "Jet Airways", "NM": "Mount Cook Airline", "DJ": "Virgin Blue",
    
    # ===== CANADA =====
    "AC": "Air Canada", "WS": "WestJet", "TS": "Air Transat", "PD": "Porter Airlines",
    "F8": "Flair Airlines", "5T": "Canadian North", "W4": "LC Peru", "4N": "Air North",
    
    # ===== LATIN AMERICA =====
    "AM": "Aeromexico", "AV": "Avianca", "CM": "Copa Airlines", "LA": "LATAM Airlines",
    "VB": "VivaAerobus", "Y4": "Volaris", "4O": "Interjet", "2F": "Frontier Flying Service",
    "AD": "Azul Brazilian Airlines", "G3": "GOL Airlines", "JJ": "LATAM Brasil",
    "AR": "Aerolineas Argentinas", "FO": "Flybondi", "WJ": "JetSMART Argentina",
    "H2": "SKY Airline", "JA": "JetSMART Chile", "4C": "LATAM Colombia",
    "LP": "LATAM Peru", "XL": "LATAM Ecuador", "4M": "LATAM Paraguay",
    "P0": "Paranair", "PZ": "LATAM Paraguay", "TA": "TACA", "N3": "Omskavia",
    "WC": "Avianca Costa Rica", "NI": "Portugalia", "5U": "TAG Airlines",
    
    # ===== AFRICA =====
    "SA": "South African Airways", "ET": "Ethiopian Airlines", "MS": "EgyptAir",
    "AT": "Royal Air Maroc", "TU": "Tunisair", "AH": "Air Algerie", "KQ": "Kenya Airways",
    "W3": "Arik Air", "KP": "ASKY Airlines", "FB": "Bulgaria Air", "FN": "fastjet",
    "TC": "Air Tanzania", "QC": "Camair-Co", "8U": "Afriqiyah Airways", "UG": "TunisAir Express",
    "BJ": "Nouvelair", "8H": "BH Air", "L6": "Mauritania Airlines International",
    "6W": "Saratov Airlines", "HF": "Air Cote d'Ivoire", "VR": "TACV",
    
    # ===== REGIONAL & OTHERS =====
    "TK": "Turkish Airlines", "PC": "Pegasus Airlines", "XQ": "SunExpress",
    "BI": "Royal Brunei Airlines", "PK": "Pakistan International Airlines",
    "BG": "Biman Bangladesh Airlines", "UL": "SriLankan Airlines", "RA": "Nepal Airlines",
    "QH": "Bamboo Airways", "H1": "Hahn Air", "U6": "Ural Airlines", "S7": "S7 Airlines",
    "SU": "Aeroflot", "DP": "Pobeda", "YC": "Yamal Airlines", "N4": "Nordwind Airlines",
    "HY": "Uzbekistan Airways", "KC": "Air Astana", "DV": "Scat Airlines",
    "B2": "Belavia", "A9": "Georgian Airways", "J2": "Azerbaijan Airlines",
}

# ══════════════════════════════════════════════════════════════════════════════
# MEAL / SSR SERVICE CODES
# ══════════════════════════════════════════════════════════════════════════════

MEAL_CODES = {
    # ── Standard IATA Meal Codes ────────────────────────────────────────────
    "VGML": "Veg Meal",
    "VCSW": "Veg Sandwich + Beverage",
    "NVML": "Non-Veg Meal",
    "CPML": "Complimentary Meal",
    "AVML": "Asian Vegetarian Meal",
    "HNML": "Hindu Non-Vegetarian Meal",
    "VLML": "Vegetarian Lacto-Ovo Meal",
    "VJML": "Vegetarian Jain Meal",
    "DBML": "Diabetic Meal",
    "LFML": "Low Fat/Low Cholesterol Meal",
    "GFML": "Gluten Free Meal",
    "LSML": "Low Salt Meal",
    "BBML": "Baby Meal",
    "CHML": "Child Meal",
    "FPML": "Fruit Platter Meal",
    "SFML": "Seafood Meal",
    "MOML": "Muslim Meal",
    "KSML": "Kosher Meal",
    "LCML": "Low Calorie Meal",
    "BLML": "Bland Meal",
    "SPML": "Special Meal",
    "VOML": "Vegetarian Oriental Meal",
    "RVML": "Raw Vegetarian Meal",
    "ORML": "Oriental Meal",
    "PRML": "Low Purine Meal",
    "LPML": "Low Protein Meal",
    "HFML": "High Fibre Meal",
    "NFML": "No Fish Meal",
    "NSML": "No Salt Meal",
    "PFML": "Peanut Free Meal",
    # ── Airline-Specific Combo / Snack Codes ────────────────────────────────
    "TCSW": "Tomato Cucumber Cheese Lettuce Sandwich Combo",
    "PTSW": "Paneer Tikka Sandwich Combo",
    "MASP": "Makhana Salt and Pepper",
    "SMAL": "Smoked Almonds",
    "TCSI": "Veggie Tomato Cucumber Sandwich",
    "CLAT": "Cucumber Cheese Lettuce Sandwich",
    "VGTR": "Veg Trio Sandwich (New)",
    "VGTI": "Veg Trio Sandwich",
    "VSUB": "Feta Cheese and Veg Sub with Dried Fruits",
    "VSUI": "Cheese and Veg Sub",
    "CNKR": "Chana Kulcha Roll",
    "BHPS": "Bhatti Paneer Salad",
    "CNWT": "Cashew (Salted)",
    "CCWT": "Unibic Chocolate Chips Cookies – 50gms",
    "SAMS": "Samosa",
    "BCCS": "Banana Chips",
    "PITA": "2 Dips with Baked Pita (New)",
    "PITI": "2 Dips with Baked Pita",
    "COMI": "Cornflakes with Milk",
    "MUYO": "Muesli with Yogurt",
    "CTSW": "Chicken Tikka Sandwich Combo",
    "CJSW": "Chicken Junglee Sandwich Combo",
    "CTAT": "Chicken Tikka Sandwich – 90gms",
    "CHSS": "Chicken Supreme Salad",
    "CHCT": "Chicken Cucumber Tomato Sandwich (New)",
    "CHCI": "Chicken Cucumber Tomato Sandwich",
}

ANCILLARY_CODES = {
    # ── Baggage ──
    "XBAG": "Extra Baggage",
    "PBAG": "Prepaid Baggage",
    "BAGP": "Baggage Priority",
    # ── Seating ──
    "SEAT": "Advance Seat Selection",
    "RQST": "Seat Request",
    "EXST": "Extra Seat",
    # ── Assistance ──
    "WCHR": "Wheelchair (Ramp)",
    "WCHS": "Wheelchair (Steps)",
    "WCHC": "Wheelchair (Carry)",
    "MAAS": "Meet and Assist",
    "UMNR": "Unaccompanied Minor",
    "BLND": "Blind Passenger",
    "DEAF": "Deaf Passenger",
    "DPNA": "Disabled Passenger Needing Assistance",
    # ── Priority / Lounge ──
    "PRIO": "Priority Boarding",
    "FAST": "Fast Pass",
    "LOUG": "Lounge Access",
    # ── Pets ──
    "PETC": "Pet in Cabin",
    "AVIH": "Animal in Hold",
    # ── Other ──
    "CPTR": "Corporate Traveller",
    "STCR": "Stretcher Passenger",
    "OXYG": "Oxygen",
    "BIKE": "Bicycle",
    "SPEQ": "Sports Equipment",
    "GOLF": "Golf Equipment",
    "FRAG": "Fragile Baggage",
    "BULK": "Bulky Baggage",
    "WBAG": "Cabin Baggage Excess",
}


# ══════════════════════════════════════════════════════════════════════════════
# BOOKING CLASS (RBD) CODES
# ══════════════════════════════════════════════════════════════════════════════
# Generic IATA fare-class mapping — most airlines follow this broadly.
# Airline-specific overrides are in _AIRLINE_CLASS_OVERRIDES.

BOOKING_CLASS_GENERIC = {
    # ── First Class ──
    "F": ("First", "First Class"),
    "A": ("First", "First Class"),
    "P": ("First", "First Class Premium"),
    # ── Business Class ──
    "C": ("Business", "Business Class"),
    "J": ("Business", "Business Class (Premium)"),
    "D": ("Business", "Business Class"),
    "I": ("Business", "Business Class"),
    "Z": ("Business", "Business Class"),
    # ── Premium Economy ──
    "W": ("Premium Economy", "Premium Economy"),
    "R": ("Premium Economy", "Premium Economy"),
    # ── Economy Class ──
    "Y": ("Economy", "Economy Class"),
    "B": ("Economy", "Economy Class"),
    "H": ("Economy", "Economy Class"),
    "K": ("Economy", "Economy Class"),
    "M": ("Economy", "Economy Class"),
    "L": ("Economy", "Economy Class"),
    "V": ("Economy", "Economy Class"),
    "S": ("Economy", "Economy Class"),
    "N": ("Economy", "Economy Class"),
    "Q": ("Economy", "Economy Class"),
    "T": ("Economy", "Economy Class"),
    "E": ("Economy", "Economy Class"),
    "U": ("Economy", "Economy Class"),
    "G": ("Economy", "Economy Class"),
    "O": ("Economy", "Economy Class"),
    "X": ("Economy", "Economy Class"),
}

# Airline-specific overrides: {airline_code: {letter: (cabin, full_form)}}
_AIRLINE_CLASS_OVERRIDES = {
    # IndiGo (6E)
    "6E": {
        "O": ("Economy", "Economy Flexi Fare"),
        "R": ("Economy", "Economy Super Saver"),
        "S": ("Economy", "Economy Saver"),
        "L": ("Economy", "Economy Lite Fare"),
        "V": ("Economy", "Economy Corporate Fare"),
    },
    # Air India (AI)
    "AI": {
        "O": ("Economy", "Economy"),
        "J": ("Business", "Business Class (Maharaja)"),
    },
    # Vistara / Air India (UK)
    "UK": {
        "Z": ("Premium Economy", "Premium Economy"),
        "O": ("Premium Economy", "Premium Economy Lite"),
    },
    # Emirates (EK)
    "EK": {
        "O": ("Business", "Business Class Saver"),
        "Z": ("First", "First Class"),
    },
    # Qatar Airways (QR)
    "QR": {
        "O": ("Business", "Business Class"),
    },
    # Singapore Airlines (SQ)
    "SQ": {
        "R": ("Premium Economy", "Premium Economy "),
        "O": ("Economy", "Economy"),
    },
    # Lufthansa (LH)
    "LH": {
        "O": ("Economy", "Economy Light"),
    },
    # SpiceJet (SG)
    "SG": {
        "O": ("Economy", "Economy Value"),
        "R": ("Economy", "Economy Flexi"),
    },
    # AirAsia India (I5)
    "I5": {
        "O": ("Economy", "Economy Value Pack"),
    },
    # Go First (G8)
    "G8": {
        "O": ("Economy", "Economy Base"),
    },
}


def resolve_booking_class(letter: str, airline_code: str = None) -> dict:
    """
    Resolve a single-letter booking class to its cabin and full form.
    Returns {"letter": "Q", "cabin": "Economy", "full_form": "Economy Class (Discounted)"}
    Uses airline-specific override if available, else falls back to generic.
    """
    letter = letter.upper().strip() if letter else ""
    if not letter or letter == "N/A":
        return {"letter": "N/A", "cabin": "N/A", "full_form": "N/A"}

    # Try airline-specific first
    if airline_code:
        ac = airline_code.upper().strip()
        overrides = _AIRLINE_CLASS_OVERRIDES.get(ac, {})
        if letter in overrides:
            cabin, full = overrides[letter]
            return {"letter": letter, "cabin": cabin, "full_form": full}

    # Generic fallback
    if letter in BOOKING_CLASS_GENERIC:
        cabin, full = BOOKING_CLASS_GENERIC[letter]
        return {"letter": letter, "cabin": cabin, "full_form": full}

    return {"letter": letter, "cabin": "Economy", "full_form": f"Economy ({letter})"}


# IANA Timezone Mapping for DST Support
AIRPORT_TZ_MAP = {
    # ===== INDIA (No DST - All UTC+5:30) =====
    "CCU": "Asia/Kolkata", "DEL": "Asia/Kolkata", "BOM": "Asia/Kolkata", "BLR": "Asia/Kolkata",
    "MAA": "Asia/Kolkata", "HYD": "Asia/Kolkata", "AMD": "Asia/Kolkata", "PNQ": "Asia/Kolkata",
    "GOI": "Asia/Kolkata", "COK": "Asia/Kolkata", "TRV": "Asia/Kolkata", "GAU": "Asia/Kolkata",
    "JAI": "Asia/Kolkata", "LKO": "Asia/Kolkata", "PAT": "Asia/Kolkata", "IXR": "Asia/Kolkata",
    "BBI": "Asia/Kolkata", "IXB": "Asia/Kolkata", "VNS": "Asia/Kolkata", "IXC": "Asia/Kolkata",
    "SXR": "Asia/Kolkata", "IXZ": "Asia/Kolkata", "VGA": "Asia/Kolkata", "IXE": "Asia/Kolkata",
    "IXM": "Asia/Kolkata", "IXU": "Asia/Kolkata", "NAG": "Asia/Kolkata", "IDR": "Asia/Kolkata",
    "RPR": "Asia/Kolkata", "IMF": "Asia/Kolkata", "DIB": "Asia/Kolkata", "JRH": "Asia/Kolkata",
    "IXJ": "Asia/Kolkata", "ATQ": "Asia/Kolkata", "IXL": "Asia/Kolkata", "UDR": "Asia/Kolkata",
    "BDQ": "Asia/Kolkata", "RAJ": "Asia/Kolkata", "STV": "Asia/Kolkata", "PBD": "Asia/Kolkata",
    "BHJ": "Asia/Kolkata", "BHO": "Asia/Kolkata", "JLR": "Asia/Kolkata", "GWL": "Asia/Kolkata",
    "AGR": "Asia/Kolkata", "IXD": "Asia/Kolkata", "VDY": "Asia/Kolkata", "RJA": "Asia/Kolkata",
    "TIR": "Asia/Kolkata", "BEP": "Asia/Kolkata", "HBX": "Asia/Kolkata", "IXG": "Asia/Kolkata",
    "GOP": "Asia/Kolkata", "DED": "Asia/Kolkata", "PGH": "Asia/Kolkata", "TNI": "Asia/Kolkata",
    "KUU": "Asia/Kolkata", "SHL": "Asia/Kolkata", "IXS": "Asia/Kolkata", "AJL": "Asia/Kolkata",
    "IXA": "Asia/Kolkata", "DMU": "Asia/Kolkata", "CBD": "Asia/Kolkata", "IXV": "Asia/Kolkata",
    "IXW": "Asia/Kolkata", "IXI": "Asia/Kolkata", "TEZ": "Asia/Kolkata", "RGH": "Asia/Kolkata",
    "COH": "Asia/Kolkata", "VTZ": "Asia/Kolkata",
    
    # ===== UNITED STATES (With DST) =====
    "ATL": "America/New_York", "JFK": "America/New_York", "EWR": "America/New_York",
    "LGA": "America/New_York", "BOS": "America/New_York", "MIA": "America/New_York",
    "FLL": "America/New_York", "PHL": "America/New_York", "DCA": "America/New_York",
    "IAD": "America/New_York", "BWI": "America/New_York", "CLT": "America/New_York",
    "MCO": "America/New_York", "TPA": "America/New_York", "DTW": "America/Detroit",
    "PIT": "America/New_York", "RDU": "America/New_York", "CVG": "America/New_York",
    "CMH": "America/New_York", "IND": "America/Indiana/Indianapolis", "BUF": "America/New_York",
    "RIC": "America/New_York", "ORF": "America/New_York", "ROC": "America/New_York",
    "SYR": "America/New_York", "ALB": "America/New_York", "BDL": "America/New_York",
    "PBI": "America/New_York", "RSW": "America/New_York", "JAX": "America/New_York",
    "MYR": "America/New_York", "CHS": "America/New_York", "SAV": "America/New_York",
    "BTV": "America/New_York", "PWM": "America/New_York", "PVD": "America/New_York",
    "MHT": "America/New_York", "ORD": "America/Chicago", "MDW": "America/Chicago",
    "DFW": "America/Chicago", "IAH": "America/Chicago", "HOU": "America/Chicago",
    "MSP": "America/Chicago", "STL": "America/Chicago", "MCI": "America/Chicago",
    "BNA": "America/Chicago", "MSY": "America/Chicago", "AUS": "America/Chicago",
    "SAT": "America/Chicago", "OMA": "America/Chicago", "DSM": "America/Chicago",
    "MKE": "America/Chicago", "GRR": "America/Detroit", "DAY": "America/New_York",
    "CAK": "America/New_York", "LEX": "America/New_York", "GSP": "America/New_York",
    "TYS": "America/New_York", "LIT": "America/Chicago", "TUL": "America/Chicago",
    "OKC": "America/Chicago", "ICT": "America/Chicago", "CID": "America/Chicago",
    "LAX": "America/Los_Angeles", "SFO": "America/Los_Angeles", "SAN": "America/Los_Angeles",
    "SEA": "America/Los_Angeles", "PDX": "America/Los_Angeles", "LAS": "America/Los_Angeles",
    "PHX": "America/Phoenix", "DEN": "America/Denver", "SLC": "America/Denver",
    "OAK": "America/Los_Angeles", "SJC": "America/Los_Angeles", "SMF": "America/Los_Angeles",
    "ONT": "America/Los_Angeles", "BUR": "America/Los_Angeles", "SNA": "America/Los_Angeles",
    "LGB": "America/Los_Angeles", "RNO": "America/Los_Angeles", "BOI": "America/Boise",
    "GEG": "America/Los_Angeles", "ANC": "America/Anchorage", "FAI": "America/Anchorage",
    "HNL": "Pacific/Honolulu", "OGG": "Pacific/Honolulu", "LIH": "Pacific/Honolulu",
    "KOA": "Pacific/Honolulu", "ITO": "Pacific/Honolulu", "ABQ": "America/Denver",
    "TUS": "America/Phoenix", "ELP": "America/Denver", "MAF": "America/Chicago",
    "FAT": "America/Los_Angeles", "PSP": "America/Los_Angeles", "SDF": "America/New_York",
    
    # ===== CANADA (With DST) =====
    "YYZ": "America/Toronto", "YUL": "America/Toronto", "YOW": "America/Toronto",
    "YHZ": "America/Halifax", "YQB": "America/Toronto", "YXU": "America/Toronto",
    "YHM": "America/Toronto", "YKF": "America/Toronto", "YQM": "America/Moncton",
    "YYT": "America/St_Johns", "YVR": "America/Vancouver", "YYC": "America/Edmonton",
    "YEG": "America/Edmonton", "YWG": "America/Winnipeg", "YXE": "America/Regina",
    "YQR": "America/Regina", "YYJ": "America/Vancouver", "YXX": "America/Vancouver",
    "YLW": "America/Vancouver", "YKA": "America/Vancouver", "YXS": "America/Vancouver",
    "YZF": "America/Yellowknife", "YXY": "America/Whitehorse", "YFB": "America/Iqaluit",
    "YQX": "America/St_Johns", "YQT": "America/Toronto",
    
    # ===== EUROPE (With DST) =====
    "LHR": "Europe/London", "LGW": "Europe/London", "STN": "Europe/London", "LTN": "Europe/London",
    "LCY": "Europe/London", "MAN": "Europe/London", "EDI": "Europe/London", "BHX": "Europe/London",
    "GLA": "Europe/London", "BFS": "Europe/Belfast", "CDG": "Europe/Paris", "ORY": "Europe/Paris",
    "NCE": "Europe/Paris", "LYS": "Europe/Paris", "MRS": "Europe/Paris", "TLS": "Europe/Paris",
    "BOD": "Europe/Paris", "NTE": "Europe/Paris", "MLH": "Europe/Paris", "BSL": "Europe/Paris",
    "FRA": "Europe/Berlin", "MUC": "Europe/Berlin", "DUS": "Europe/Berlin", "HAM": "Europe/Berlin",
    "BER": "Europe/Berlin", "STR": "Europe/Berlin", "CGN": "Europe/Berlin", "BRE": "Europe/Berlin",
    "HAJ": "Europe/Berlin", "NUE": "Europe/Berlin", "LEJ": "Europe/Berlin", "DRS": "Europe/Berlin",
    "FMM": "Europe/Berlin", "AMS": "Europe/Amsterdam", "ZRH": "Europe/Zurich", "GVA": "Europe/Zurich",
    "VIE": "Europe/Vienna", "BRU": "Europe/Brussels", "CPH": "Europe/Copenhagen",
    "OSL": "Europe/Oslo", "ARN": "Europe/Stockholm", "GOT": "Europe/Stockholm",
    "HEL": "Europe/Helsinki", "DUB": "Europe/Dublin", "SNN": "Europe/Dublin", "ORK": "Europe/Dublin",
    "MAD": "Europe/Madrid", "BCN": "Europe/Madrid", "AGP": "Europe/Madrid", "PMI": "Europe/Madrid",
    "SVQ": "Europe/Madrid", "VLC": "Europe/Madrid", "ALC": "Europe/Madrid", "IBZ": "Europe/Madrid",
    "MAH": "Europe/Madrid", "BIO": "Europe/Madrid", "SCQ": "Europe/Madrid",
    "LIS": "Europe/Lisbon", "OPO": "Europe/Lisbon", "FAO": "Europe/Lisbon", "FNC": "Atlantic/Madeira",
    "PDL": "Atlantic/Azores", "FCO": "Europe/Rome", "MXP": "Europe/Rome", "LIN": "Europe/Rome",
    "VCE": "Europe/Rome", "BGY": "Europe/Rome", "BLQ": "Europe/Rome", "NAP": "Europe/Rome",
    "CAG": "Europe/Rome", "CTA": "Europe/Rome", "PMO": "Europe/Rome", "BRI": "Europe/Rome",
    "ATH": "Europe/Athens", "SKG": "Europe/Athens", "RHO": "Europe/Athens", "HER": "Europe/Athens",
    "CFU": "Europe/Athens", "CHQ": "Europe/Athens", "JTR": "Europe/Athens", "ZTH": "Europe/Athens",
    "KGS": "Europe/Athens", "IST": "Europe/Istanbul", "SAW": "Europe/Istanbul", "AYT": "Europe/Istanbul",
    "ADA": "Europe/Istanbul", "ESB": "Europe/Istanbul", "ADB": "Europe/Istanbul", "BJV": "Europe/Istanbul",
    "DLM": "Europe/Istanbul", "GZT": "Europe/Istanbul", "ASR": "Europe/Istanbul",
    "PRG": "Europe/Prague", "BUD": "Europe/Budapest", "WAW": "Europe/Warsaw", "KRK": "Europe/Warsaw",
    "GDN": "Europe/Warsaw", "WRO": "Europe/Warsaw", "KTW": "Europe/Warsaw", "POZ": "Europe/Warsaw",
    "OTP": "Europe/Bucharest", "CLJ": "Europe/Bucharest", "IAS": "Europe/Bucharest",
    "BTS": "Europe/Bratislava", "SOF": "Europe/Sofia", "BEG": "Europe/Belgrade",
    "ZAG": "Europe/Zagreb", "LJU": "Europe/Ljubljana", "SPU": "Europe/Belgrade", "DBV": "Europe/Belgrade",
    "VNO": "Europe/Vilnius", "RIX": "Europe/Riga", "TLL": "Europe/Tallinn",
    "LED": "Europe/Moscow", "SVO": "Europe/Moscow", "DME": "Europe/Moscow",
    "LYX": "Asia/Nicosia", "PFO": "Asia/Nicosia",
    
    # ===== MIDDLE EAST =====
    "DXB": "Asia/Dubai", "AUH": "Asia/Dubai", "SHJ": "Asia/Dubai", "DOH": "Asia/Qatar",
    "MCT": "Asia/Muscat", "BAH": "Asia/Bahrain", "KWI": "Asia/Kuwait",
    "RUH": "Asia/Riyadh", "JED": "Asia/Riyadh", "DMM": "Asia/Riyadh", "MED": "Asia/Riyadh",
    "TIF": "Asia/Riyadh", "GIZ": "Asia/Riyadh", "AJF": "Asia/Riyadh", "ELQ": "Asia/Riyadh",
    "HAS": "Asia/Riyadh", "TUU": "Asia/Riyadh", "AHB": "Asia/Riyadh", "URY": "Asia/Riyadh",
    "RAE": "Asia/Riyadh", "CAI": "Africa/Cairo", "SSH": "Africa/Cairo", "HRG": "Africa/Cairo",
    "LXR": "Africa/Cairo", "ASW": "Africa/Cairo", "TLV": "Asia/Jerusalem",
    "AMM": "Asia/Amman", "BEY": "Asia/Beirut", "BGW": "Asia/Baghdad", "BSR": "Asia/Baghdad",
    "EBL": "Asia/Baghdad", "IKA": "Asia/Tehran", "THR": "Asia/Tehran", "MHD": "Asia/Tehran",
    "SYZ": "Asia/Tehran", "TBZ": "Asia/Tehran",
    
    # ===== EAST ASIA =====
    "PEK": "Asia/Shanghai", "PVG": "Asia/Shanghai", "CAN": "Asia/Shanghai", "SHA": "Asia/Shanghai",
    "CTU": "Asia/Shanghai", "SZX": "Asia/Shanghai", "KMG": "Asia/Shanghai", "XIY": "Asia/Shanghai",
    "CKG": "Asia/Shanghai", "HGH": "Asia/Shanghai", "NKG": "Asia/Shanghai", "WUH": "Asia/Shanghai",
    "TSN": "Asia/Shanghai", "TAO": "Asia/Shanghai", "DLC": "Asia/Shanghai", "SHE": "Asia/Shanghai",
    "CGO": "Asia/Shanghai", "XMN": "Asia/Shanghai", "FOC": "Asia/Shanghai", "CSX": "Asia/Shanghai",
    "NNG": "Asia/Shanghai", "KWL": "Asia/Shanghai", "CGQ": "Asia/Shanghai", "HRB": "Asia/Shanghai",
    "URC": "Asia/Urumqi", "LHW": "Asia/Shanghai", "HET": "Asia/Shanghai",
    "HKG": "Asia/Hong_Kong", "MFM": "Asia/Macau", "TPE": "Asia/Taipei", "TSA": "Asia/Taipei",
    "KHH": "Asia/Taipei", "RMQ": "Asia/Taipei", "NRT": "Asia/Tokyo", "HND": "Asia/Tokyo",
    "KIX": "Asia/Tokyo", "ITM": "Asia/Tokyo", "NGO": "Asia/Tokyo", "FUK": "Asia/Tokyo",
    "CTS": "Asia/Tokyo", "OKA": "Asia/Tokyo", "KOJ": "Asia/Tokyo", "HIJ": "Asia/Tokyo",
    "ICN": "Asia/Seoul", "GMP": "Asia/Seoul", "PUS": "Asia/Seoul", "CJU": "Asia/Seoul",
    
    # ===== SOUTHEAST ASIA =====
    "SIN": "Asia/Singapore", "BKK": "Asia/Bangkok", "DMK": "Asia/Bangkok", "CNX": "Asia/Bangkok",
    "HKT": "Asia/Bangkok", "USM": "Asia/Bangkok", "KBV": "Asia/Bangkok", "HDY": "Asia/Bangkok",
    "CEI": "Asia/Bangkok", "UTP": "Asia/Bangkok", "URT": "Asia/Bangkok",
    "CGK": "Asia/Jakarta", "SUB": "Asia/Jakarta", "DPS": "Asia/Makassar", "JOG": "Asia/Jakarta",
    "BDO": "Asia/Jakarta", "MDC": "Asia/Makassar", "UPG": "Asia/Makassar", "BPN": "Asia/Makassar",
    "PLM": "Asia/Jakarta", "PKU": "Asia/Jakarta", "BTH": "Asia/Jakarta", "SRG": "Asia/Jakarta",
    "SOC": "Asia/Jakarta", "KUL": "Asia/Kuala_Lumpur", "SZB": "Asia/Kuala_Lumpur",
    "PEN": "Asia/Kuala_Lumpur", "JHB": "Asia/Kuala_Lumpur", "KCH": "Asia/Kuching",
    "BKI": "Asia/Kuala_Lumpur", "LGK": "Asia/Kuala_Lumpur",
    "MNL": "Asia/Manila", "CEB": "Asia/Manila", "DVO": "Asia/Manila", "ILO": "Asia/Manila",
    "CRK": "Asia/Manila", "KLO": "Asia/Manila", "MPH": "Asia/Manila", "BCD": "Asia/Manila",
    "CBO": "Asia/Manila", "GES": "Asia/Manila", "HAN": "Asia/Ho_Chi_Minh", "SGN": "Asia/Ho_Chi_Minh",
    "DAD": "Asia/Ho_Chi_Minh", "CXR": "Asia/Ho_Chi_Minh", "DLI": "Asia/Ho_Chi_Minh",
    "VII": "Asia/Ho_Chi_Minh", "HUI": "Asia/Ho_Chi_Minh", "PQC": "Asia/Ho_Chi_Minh",
    "VCA": "Asia/Ho_Chi_Minh", "BMV": "Asia/Ho_Chi_Minh", "RGN": "Asia/Yangon",
    "MDL": "Asia/Yangon", "NYU": "Asia/Yangon", "HEH": "Asia/Yangon",
    "PNH": "Asia/Phnom_Penh", "REP": "Asia/Phnom_Penh", "KOS": "Asia/Phnom_Penh",
    "VTE": "Asia/Vientiane", "LPQ": "Asia/Vientiane", "PKZ": "Asia/Vientiane",
    "BWN": "Asia/Brunei",
    
    # ===== SOUTH ASIA =====
    "CMB": "Asia/Colombo", "DAC": "Asia/Dhaka", "RJH": "Asia/Dhaka", "CXB": "Asia/Dhaka",
    "CGP": "Asia/Dhaka", "JSR": "Asia/Dhaka", "ZYL": "Asia/Dhaka",
    "KTM": "Asia/Kathmandu", "PKR": "Asia/Kathmandu", "BWA": "Asia/Kathmandu", "BIR": "Asia/Kathmandu",
    "PBH": "Asia/Thimphu", "BUT": "Asia/Thimphu", "YON": "Asia/Thimphu", "GLU": "Asia/Thimphu",
    "MLE": "Indian/Maldives", "GAN": "Indian/Maldives", "KDM": "Indian/Maldives",
    "ISB": "Asia/Karachi", "KHI": "Asia/Karachi", "LHE": "Asia/Karachi", "SKT": "Asia/Karachi",
    "PEW": "Asia/Karachi", "MUX": "Asia/Karachi", "UET": "Asia/Karachi",
    "KBL": "Asia/Kabul",
    
    # ===== OCEANIA (With DST) =====
    "SYD": "Australia/Sydney", "MEL": "Australia/Melbourne", "BNE": "Australia/Brisbane",
    "PER": "Australia/Perth", "ADL": "Australia/Adelaide", "CNS": "Australia/Brisbane",
    "OOL": "Australia/Brisbane", "CBR": "Australia/Sydney", "HBA": "Australia/Hobart",
    "DRW": "Australia/Darwin", "AVV": "Australia/Melbourne", "MCY": "Australia/Brisbane",
    "BNK": "Australia/Sydney", "LST": "Australia/Hobart", "ASP": "Australia/Darwin",
    "TSV": "Australia/Brisbane", "ROK": "Australia/Brisbane", "MKY": "Australia/Brisbane",
    "HTI": "Australia/Brisbane", "PPP": "Australia/Brisbane", "ABX": "Australia/Sydney",
    "OAG": "Australia/Sydney", "ARM": "Australia/Sydney", "TMW": "Australia/Sydney",
    "AKL": "Pacific/Auckland", "WLG": "Pacific/Auckland", "CHC": "Pacific/Auckland",
    "ZQN": "Pacific/Auckland", "DUD": "Pacific/Auckland", "NSN": "Pacific/Auckland",
    "NPL": "Pacific/Auckland", "PMR": "Pacific/Auckland", "TRG": "Pacific/Auckland",
    "ROT": "Pacific/Auckland", "NPE": "Pacific/Auckland", "BHE": "Pacific/Auckland",
    "IVC": "Pacific/Auckland", "NOU": "Pacific/Noumea", "PPT": "Pacific/Tahiti",
    "NAN": "Pacific/Fiji", "VLI": "Pacific/Efate", "HIR": "Pacific/Guadalcanal",
    "APW": "Pacific/Apia", "POM": "Pacific/Port_Moresby", "GUM": "Pacific/Guam",
    "SPN": "Pacific/Saipan", "TRW": "Pacific/Tarawa", "MAJ": "Pacific/Majuro",
    "KWA": "Pacific/Kwajalein", "PNI": "Pacific/Pohnpei", "TKK": "Pacific/Chuuk",
    "ROR": "Pacific/Palau", "YAP": "Pacific/Chuuk",
    
    # ===== LATIN AMERICA =====
    "MEX": "America/Mexico_City", "GDL": "America/Mexico_City", "MTY": "America/Monterrey",
    "TIJ": "America/Tijuana", "CUN": "America/Cancun", "CUU": "America/Chihuahua",
    "HMO": "America/Hermosillo", "BJX": "America/Mexico_City", "PVR": "America/Mexico_City",
    "SJD": "America/Mazatlan", "MID": "America/Merida", "CZM": "America/Cancun",
    "ZIH": "America/Mexico_City", "ACA": "America/Mexico_City", "TAP": "America/Mexico_City",
    "VER": "America/Mexico_City", "OAX": "America/Mexico_City",
    "GRU": "America/Sao_Paulo", "GIG": "America/Sao_Paulo", "BSB": "America/Sao_Paulo",
    "CGH": "America/Sao_Paulo", "VCP": "America/Sao_Paulo", "SDU": "America/Sao_Paulo",
    "CWB": "America/Sao_Paulo", "POA": "America/Sao_Paulo", "FOR": "America/Fortaleza",
    "REC": "America/Recife", "SSA": "America/Bahia", "BEL": "America/Belem",
    "MAO": "America/Manaus", "CGB": "America/Cuiaba", "GYN": "America/Sao_Paulo",
    "BHZ": "America/Sao_Paulo", "CNF": "America/Sao_Paulo", "VIX": "America/Sao_Paulo",
    "FLN": "America/Sao_Paulo", "NAT": "America/Fortaleza", "MCZ": "America/Maceio",
    "AJU": "America/Maceio", "THE": "America/Fortaleza",
    "EZE": "America/Argentina/Buenos_Aires", "AEP": "America/Argentina/Buenos_Aires",
    "COR": "America/Argentina/Cordoba", "MDZ": "America/Argentina/Mendoza",
    "ROS": "America/Argentina/Cordoba", "BRC": "America/Argentina/Salta",
    "USH": "America/Argentina/Ushuaia", "IGR": "America/Argentina/Jujuy",
    "SCL": "America/Santiago", "CCP": "America/Santiago", "ANF": "America/Santiago",
    "IQQ": "America/Santiago", "LSC": "America/Santiago", "ZCO": "America/Santiago",
    "PUQ": "America/Punta_Arenas", "BOG": "America/Bogota", "MDE": "America/Bogota",
    "CTG": "America/Bogota", "CLO": "America/Bogota", "BAQ": "America/Bogota",
    "BGA": "America/Bogota", "SMR": "America/Bogota", "ADZ": "America/Bogota",
    "PEI": "America/Bogota", "CUC": "America/Bogota", "LIM": "America/Lima",
    "GYE": "America/Guayaquil", "UIO": "America/Guayaquil",
    "LPB": "America/La_Paz", "VVI": "America/La_Paz", "CBB": "America/La_Paz",
    "ASU": "America/Asuncion", "MVD": "America/Montevideo",
    "CCS": "America/Caracas", "VLN": "America/Caracas", "MAR": "America/Caracas", "BLA": "America/Caracas",
    "PTY": "America/Panama", "SJO": "America/Costa_Rica", "LIR": "America/Costa_Rica",
    "SAL": "America/El_Salvador", "GUA": "America/Guatemala", "TGU": "America/Tegucigalpa",
    "SAP": "America/Tegucigalpa", "RTB": "America/Tegucigalpa", "MGA": "America/Managua",
    "BZE": "America/Belize", "HAV": "America/Havana", "PUJ": "America/Santo_Domingo",
    "SDQ": "America/Santo_Domingo", "SJU": "America/Puerto_Rico", "NAS": "America/Nassau",
    "KIN": "America/Jamaica", "MBJ": "America/Jamaica", "AUA": "America/Aruba",
    "CUR": "America/Curacao", "BON": "America/Kralendijk", "POS": "America/Port_of_Spain",
    "BGI": "America/Barbados", "GND": "America/Grenada", "UVF": "America/St_Lucia",
    "SXM": "America/Lower_Princes", "SKB": "America/St_Kitts", "ANU": "America/Antigua",
    "GCM": "America/Cayman", "PBM": "America/Paramaribo", "CAY": "America/Cayenne",
    "GEO": "America/Guyana",
    
    # ===== AFRICA =====
    "JNB": "Africa/Johannesburg", "CPT": "Africa/Johannesburg", "DUR": "Africa/Johannesburg",
    "PLZ": "Africa/Johannesburg", "GRJ": "Africa/Johannesburg",
    "ADD": "Africa/Addis_Ababa", "NBO": "Africa/Nairobi", "MBA": "Africa/Nairobi",
    "JRO": "Africa/Nairobi", "DAR": "Africa/Dar_es_Salaam", "ZNZ": "Africa/Dar_es_Salaam",
    "EBB": "Africa/Kampala", "KGL": "Africa/Kigali", "BJM": "Africa/Bujumbura",
    "FIH": "Africa/Kinshasa", "WDH": "Africa/Windhoek", "VFA": "Africa/Harare",
    "HRE": "Africa/Harare", "LUN": "Africa/Lusaka", "BLZ": "Africa/Blantyre",
    "LOS": "Africa/Lagos", "ABV": "Africa/Lagos", "KAN": "Africa/Lagos", "PHC": "Africa/Lagos",
    "ACC": "Africa/Accra", "ABJ": "Africa/Abidjan", "DKR": "Africa/Dakar", "DSS": "Africa/Dakar",
    "DLA": "Africa/Douala", "NSI": "Africa/Douala", "LBV": "Africa/Libreville",
    "COO": "Africa/Porto-Novo", "LFW": "Africa/Lome", "OUA": "Africa/Ouagadougou",
    "FNA": "Africa/Freetown", "ROB": "Africa/Monrovia", "BJL": "Africa/Banjul",
    "NKC": "Africa/Nouakchott", "ASM": "Africa/Asmara", "JIB": "Africa/Djibouti",
    "MGQ": "Africa/Mogadishu", "HGA": "Africa/Mogadishu",
    "CAI": "Africa/Cairo", "SSH": "Africa/Cairo", "HRG": "Africa/Cairo", "LXR": "Africa/Cairo",
    "ASW": "Africa/Cairo", "CMN": "Africa/Casablanca", "RAK": "Africa/Casablanca",
    "FEZ": "Africa/Casablanca", "TNG": "Africa/Casablanca", "NDR": "Africa/Casablanca",
    "AGA": "Africa/Casablanca", "OZZ": "Africa/Casablanca", "ESU": "Africa/Casablanca",
    "ALG": "Africa/Algiers", "ORN": "Africa/Algiers", "CZL": "Africa/Algiers",
    "AAE": "Africa/Algiers", "TLM": "Africa/Algiers", "BSK": "Africa/Algiers",
    "TUN": "Africa/Tunis", "SFA": "Africa/Tunis", "DJE": "Africa/Tunis", "MIR": "Africa/Tunis",
    "TIP": "Africa/Tripoli", "MJI": "Africa/Tripoli", "BEN": "Africa/Tripoli",
    "MRU": "Indian/Mauritius", "RUN": "Indian/Reunion", "SEZ": "Indian/Mahe",
    "TNR": "Indian/Antananarivo", "NOS": "Indian/Antananarivo", "MJN": "Indian/Antananarivo",
    
    # ===== CENTRAL ASIA =====
    "TAS": "Asia/Tashkent", "SKD": "Asia/Samarkand", "BHK": "Asia/Samarkand",
    "ALA": "Asia/Almaty", "NQZ": "Asia/Almaty", "FRU": "Asia/Bishkek",
    "DYU": "Asia/Dushanbe", "ASB": "Asia/Ashgabat",
    "TBS": "Asia/Tbilisi", "EVN": "Asia/Yerevan", "GYD": "Asia/Baku",
    "ISU": "Asia/Baghdad",
}

# Helper function to get airport name
def get_airport_name(code):
    """Get airport name from code"""
    return AIRPORT_CODES.get(code.upper(), code)

# Helper function to get airline name
def get_airline_name(code):
    """Get airline name from code"""
    return AIRLINE_CODES.get(code.upper(), code)

# Helper function to get timezone
def get_airport_timezone(code):
    """Get IANA timezone for airport"""
    return AIRPORT_TZ_MAP.get(code.upper(), "UTC")

# Search function for airport code
def search_airport_code(code):
    """
    Search for airport code and return all available information.
    
    Args:
        code (str): 3-letter IATA airport code (e.g., 'DEL', 'JFK')
    
    Returns:
        dict: Dictionary containing airport details or error message
    
    Example:
        >>> result = search_airport_code('DEL')
        >>> print(result)
        {
            'exists': True,
            'code': 'DEL',
            'name': 'Delhi',
            'timezone': 'Asia/Kolkata',
            'has_timezone': True
        }
    """
    code = code.upper().strip()
    
    # Validate code format
    if len(code) != 3:
        return {
            'exists': False,
            'error': 'Invalid airport code format. Must be 3 letters.',
            'code': code
        }
    
    # Check if airport exists
    if code not in AIRPORT_CODES:
        return {
            'exists': False,
            'error': f'Airport code "{code}" not found in database.',
            'code': code,
            'suggestion': 'Please check the code or add it to AIRPORT_CODES dictionary.'
        }
    
    # Get airport details
    airport_name = AIRPORT_CODES[code]
    timezone = AIRPORT_TZ_MAP.get(code)
    
    result = {
        'exists': True,
        'code': code,
        'name': airport_name,
        'timezone': timezone if timezone else 'Not mapped',
        'has_timezone': timezone is not None
    }
    
    # Add warning if timezone is missing
    if not timezone:
        result['warning'] = f'Airport "{code}" exists but timezone mapping is missing.'
    
    return result

# Bulk search function
def search_multiple_airports(codes):
    """
    Search for multiple airport codes at once.
    
    Args:
        codes (list): List of airport codes
    
    Returns:
        dict: Dictionary with each code as key and search result as value
    
    Example:
        >>> results = search_multiple_airports(['DEL', 'JFK', 'XXX'])
        >>> for code, info in results.items():
        ...     print(f"{code}: {info['exists']}")
    """
    results = {}
    for code in codes:
        results[code.upper()] = search_airport_code(code)
    return results

# Search by city/airport name (reverse lookup)
def search_by_name(search_term):
    """
    Search for airports by city or airport name (case-insensitive).
    
    Args:
        search_term (str): City or airport name to search for
    
    Returns:
        list: List of matching airports with their codes
    
    Example:
        >>> results = search_by_name('london')
        >>> for airport in results:
        ...     print(f"{airport['code']}: {airport['name']}")
    """
    search_term = search_term.lower()
    matches = []
    
    for code, name in AIRPORT_CODES.items():
        if search_term in name.lower():
            matches.append({
                'code': code,
                'name': name,
                'timezone': AIRPORT_TZ_MAP.get(code, 'Not mapped')
            })
    
    return matches

# Validation function
def validate_mapping():
    """Validate that all airports have timezone mappings"""
    airports_without_tz = []
    for code in AIRPORT_CODES:
        if code not in AIRPORT_TZ_MAP:
            airports_without_tz.append(code)
    
    if airports_without_tz:
        print(f"⚠️  Warning: {len(airports_without_tz)} airports missing timezone mappings:")
        print(airports_without_tz[:20])  # Show first 20
    else:
        print("✓ All airports have timezone mappings")
    
    print(f"\n📊 Statistics:")
    print(f"   Total Airports: {len(AIRPORT_CODES)}")
    print(f"   Total Airlines: {len(AIRLINE_CODES)}")
    print(f"   Total Timezones: {len(AIRPORT_TZ_MAP)}")

# Example usage and testing
if __name__ == "__main__":
    validate_mapping()
    
    print("\n" + "="*60)
    print("EXAMPLE USAGE - AIRPORT CODE SEARCH")
    print("="*60)
    
    # Example 1: Search for existing airport
    print("\n1. Search for Delhi (DEL):")
    result = search_airport_code('DEL')
    print(f"   Exists: {result['exists']}")
    print(f"   Name: {result['name']}")
    print(f"   Timezone: {result['timezone']}")
    
    # Example 2: Search for non-existent airport
    print("\n2. Search for invalid code (XXX):")
    result = search_airport_code('XXX')
    print(f"   Exists: {result['exists']}")
    print(f"   Error: {result.get('error', 'N/A')}")
    
    # Example 3: Search multiple airports
    print("\n3. Search multiple airports:")
    codes = ['JFK', 'LHR', 'DXB', 'SIN', 'HKG']
    results = search_multiple_airports(codes)
    for code, info in results.items():
        status = "✓" if info['exists'] else "✗"
        print(f"   {status} {code}: {info.get('name', 'Not found')}")
    
    # Example 4: Search by city name
    print("\n4. Search by city name 'Mumbai':")
    matches = search_by_name('Mumbai')
    for airport in matches:
        print(f"   {airport['code']}: {airport['name']} ({airport['timezone']})")
    
    # Example 5: Search by partial name
    print("\n5. Search by partial name 'London':")
    matches = search_by_name('London')
    for airport in matches:
        print(f"   {airport['code']}: {airport['name']}")
    
    print("\n" + "="*60)
COUNTRY_CODES = {
    # ===== ASIA =====
    "AFG": "Afghanistan", "ARM": "Armenia", "AZE": "Azerbaijan", "BHR": "Bahrain",
    "BGD": "Bangladesh", "BTN": "Bhutan", "BRN": "Brunei", "KHM": "Cambodia",
    "CHN": "China", "HKG": "Hong Kong", "MAC": "Macau", "TWN": "Taiwan",
    "GEO": "Georgia", "IND": "India", "IDN": "Indonesia", "IRN": "Iran",
    "IRQ": "Iraq", "ISR": "Israel", "JPN": "Japan", "JOR": "Jordan",
    "KAZ": "Kazakhstan", "KWT": "Kuwait", "KGZ": "Kyrgyzstan", "LAO": "Laos",
    "LBN": "Lebanon", "MYS": "Malaysia", "MDV": "Maldives", "MNG": "Mongolia",
    "MMR": "Myanmar", "NPL": "Nepal", "PRK": "North Korea", "OMN": "Oman",
    "PAK": "Pakistan", "PSE": "Palestine", "PHL": "Philippines", "QAT": "Qatar",
    "SAU": "Saudi Arabia", "SGP": "Singapore", "KOR": "South Korea", "LKA": "Sri Lanka",
    "SYR": "Syria", "TJK": "Tajikistan", "THA": "Thailand", "TLS": "Timor-Leste",
    "TUR": "Turkey", "TKM": "Turkmenistan", "ARE": "United Arab Emirates",
    "UZB": "Uzbekistan", "VNM": "Vietnam", "YEM": "Yemen",

    # ===== EUROPE =====
    "ALB": "Albania", "AND": "Andorra", "AUT": "Austria", "BLR": "Belarus",
    "BEL": "Belgium", "BIH": "Bosnia and Herzegovina", "BGR": "Bulgaria",
    "HRV": "Croatia", "CYP": "Cyprus", "CZE": "Czech Republic", "DNK": "Denmark",
    "EST": "Estonia", "FIN": "Finland", "FRA": "France", "DEU": "Germany",
    "GRC": "Greece", "HUN": "Hungary", "ISL": "Iceland", "IRL": "Ireland",
    "ITA": "Italy", "LVA": "Latvia", "LIE": "Liechtenstein", "LTU": "Lithuania",
    "LUX": "Luxembourg", "MLT": "Malta", "MDA": "Moldova", "MCO": "Monaco",
    "MNE": "Montenegro", "NLD": "Netherlands", "MKD": "North Macedonia",
    "NOR": "Norway", "POL": "Poland", "PRT": "Portugal", "ROU": "Romania",
    "RUS": "Russia", "SMR": "San Marino", "SRB": "Serbia", "SVK": "Slovakia",
    "SVN": "Slovenia", "ESP": "Spain", "SWE": "Sweden", "CHE": "Switzerland",
    "UKR": "Ukraine", "GBR": "United Kingdom", "VAT": "Vatican City",
    
    # ===== AMERICAS =====
    "ATG": "Antigua and Barbuda", "ARG": "Argentina", "BHS": "Bahamas", "BRB": "Barbados",
    "BLZ": "Belize", "BOL": "Bolivia", "BRA": "Brazil", "CAN": "Canada",
    "CHL": "Chile", "COL": "Colombia", "CRI": "Costa Rica", "CUB": "Cuba",
    "DMA": "Dominica", "DOM": "Dominican Republic", "ECU": "Ecuador", "SLV": "El Salvador",
    "GRD": "Grenada", "GTM": "Guatemala", "GUY": "Guyana", "HTI": "Haiti",
    "HND": "Honduras", "JAM": "Jamaica", "MEX": "Mexico", "NIC": "Nicaragua",
    "PAN": "Panama", "PRY": "Paraguay", "PER": "Peru", "KNA": "Saint Kitts and Nevis",
    "LCA": "Saint Lucia", "VCT": "Saint Vincent and the Grenadines", "SUR": "Suriname",
    "TTO": "Trinidad and Tobago", "USA": "United States", "URY": "Uruguay", "VEN": "Venezuela",

    # ===== AFRICA =====
    "DZA": "Algeria", "AGO": "Angola", "BEN": "Benin", "BWA": "Botswana",
    "BFA": "Burkina Faso", "BDI": "Burundi", "CMR": "Cameroon", "CPV": "Cape Verde",
    "CAF": "Central African Republic", "TCD": "Chad", "COM": "Comoros",
    "COD": "Congo (DRC)", "COG": "Congo (Republic)", "CIV": "Ivory Coast",
    "DJI": "Djibouti", "EGY": "Egypt", "GNQ": "Equatorial Guinea", "ERI": "Eritrea",
    "ETH": "Ethiopia", "GAB": "Gabon", "GMB": "Gambia", "GHA": "Ghana",
    "GIN": "Guinea", "GNB": "Guinea-Bissau", "KEN": "Kenya", "LSO": "Lesotho",
    "LBR": "Liberia", "LBY": "Libya", "MDG": "Madagascar", "MWI": "Malawi",
    "MLI": "Mali", "MRT": "Mauritania", "MUS": "Mauritius", "MAR": "Morocco",
    "MOZ": "Mozambique", "NAM": "Namibia", "NER": "Niger", "NGA": "Nigeria",
    "RWA": "Rwanda", "STP": "Sao Tome and Principe", "SEN": "Senegal",
    "SYC": "Seychelles", "SLE": "Sierra Leone", "SOM": "Somalia", "ZAF": "South Africa",
    "SSD": "South Sudan", "SDN": "Sudan", "SWZ": "Eswatini", "TZA": "Tanzania",
    "TGO": "Togo", "TUN": "Tunisia", "UGA": "Uganda", "ZMB": "Zambia", "ZWE": "Zimbabwe",

    # ===== OCEANIA =====
    "AUS": "Australia", "FJI": "Fiji", "KIR": "Kiribati", "MHL": "Marshall Islands",
    "FSM": "Micronesia", "NRU": "Nauru", "NZL": "New Zealand", "PLW": "Palau",
    "PNG": "Papua New Guinea", "WSM": "Samoa", "SLB": "Solomon Islands",
    "TON": "Tonga", "TUV": "Tuvalu", "VUT": "Vanuatu",

    # ===== ICAO Variations (Passports often use these) =====
    "D": "Germany",
    "GB": "United Kingdom", 
    "US": "United States",
    "IN": "India",
    "UNO": "United Nations",
    "UNA": "United Nations Agency"
}
