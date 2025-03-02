import os
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import boto3
from io import StringIO
from data_cleaning import *

# Directory for saving CSV files
raw_save_dir = "./sample_DB/raw_data"
clean_save_dir = "./sample_DB/clean_data"
output_excel_file = "portal_recon.xlsx"

# Ensure the directories exist
os.makedirs(raw_save_dir, exist_ok=True)
os.makedirs(clean_save_dir, exist_ok=True)

# Conference-specific configurations
conferences = [
    {
        "name": "Big Ten",
        "url": "https://www.sports-reference.com/cbb/conferences/big-ten/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Big Ten).csv",
        "name_mapping": {
            'LJ Cason': 'L.J. Cason',
            'Howard Jace': 'Jace Howard',
            'Harrison Hochenberg': 'Harrison Hochberg',
            'Kachi Nzech': 'Kachi Nzeh',
            'Jay Young': 'Jayhlon Young',
            'CJ Cox': 'C.J. Cox',
            'Angelo Ciaravino': 'Angelo Ciarvino',
            'Elohim Isaiah': 'Isaiah Elohim',
            'Keaton Kuthcer': 'Keaton Kutcher',
            'Ogbole Emmanuel': 'Emmanuel Ogbole',
            'PJ Hayes IV': 'PJ Hayes'
        }
    },
    {
        "name": "SEC",
        "url": "https://www.sports-reference.com/cbb/conferences/sec/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(SEC).csv",
        "name_mapping": {'Trevor Brazile': 'Trevon Brazile',
            'Sam Alexis ': 'Sam Alexis',
            'Colin Chandler': 'Collin Chandler',
            'Noah Boyde ': 'Noah Boyde',
            'Adam Benhayoune ': 'Adam Benhayoune',
            'Trent Pierce ': 'Trent Pierce',
            'Jacolb Fredson-Cole': 'Jacolb Cole',
            'Duncan Campbell': 'Campbell Duncan',
            'Gavin Paul': 'Gavin Paull',
            'Dubar Darilstone': 'Darlinstone Dubar',
            'Ze\'rik Onyema': 'Ze\'Rik Onyema',
            'CJ Wilcher': 'C.J. Wilcher',
            'Christopher McDermott': 'Chris McDermott',
            'JQ Roberts': 'JaQualon "JQ" Roberts'
        }
    },
    {
        "name": "Big 12",
        "url": "https://www.sports-reference.com/cbb/conferences/big-12/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(BIG 12).csv",
        "name_mapping": {
            'CJ Fredrick Jr.': 'CJ Fredrick',
            'Sebastian Ranick': 'Sebastian Rancik',
            'Patrick Suemnick': 'Pat Suemnick',
            'Drew Mcelroy': 'Drew McElroy',
            'Dior Johnson ': 'Dior Johnson',
            'Ayomide Basmisile': 'Ayomide Bamisile',
            'Brandy Smith': 'Brady Smith'
        }
    },
    {
        "name": "ACC",
        "url": "https://www.sports-reference.com/cbb/conferences/acc/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(ACC).csv",
        "name_mapping": {
            'Brandon Cummings': 'Brandin Cummings',
            'Aj George': 'AJ George',
            'Anthony Batson Jr. ': 'Anthony Batson Jr.',
            'Patrick Ngongba II': 'Patrick Ngongba',
            'Churchhill Abass': 'Churchill Abass',
            'Dennis Parker Jr. ': 'Dennis Parker Jr.',
            'Bryce Heard ': 'Bryce Heard',
            'Dante Mayo Jr': 'Dante Mayo Jr.',
            'Ty Claude': 'Tyzhaun Claude',
            'Sir Mohammed ': 'Sir Mohammed',
            'Nikita Konstantynovskti': 'Nikita Konstantynovskyi',
            'Garret Sundra': 'Garrett Sundra',
            'Jesse Jones ': 'Jesse Jones',
            'Anastasios Rozakeas ': 'Anastasios Rozakeas',
            'A.J. Swinton': 'AJ Swinton',
            'Alier Maluk ': 'Alier Maluk',
            'Jayden Karapetian ': 'Jayden Karapetian',
            'Jack Didonna ': 'Jack DiDonna',
            'Will Eggemeier ': 'Will Eggemeier',
            'Ethan Soares': 'Ethan Soares Rodriguez',
            'Aidan McCool ': 'Aidan McCool',
            'Frank Anselem-Ibe': 'Frank Anselem',
            'Khnai Rooths': 'Khani Rooths',
            'Ishan Sharma ': 'Ishan Sharma',
            'Chance Wesley': 'Chance Westry',
            'Petar Majstorvic': 'Petar Majstorovic',
            'Divine Ugochukwu ': 'Divine Ugochukwu',
            'Jalil Bethea ': 'Jalil Bethea',
            'Paul Djobet ': 'Paul Djobet',
            'Ryan Jones Jr. ': 'Ryan Jones Jr.',
            'Paul Mcneil Jr.': 'Paul McNeil Jr.'
        }
    },
    {
        "name": "A-Sun",
        "url": "https://www.sports-reference.com/cbb/conferences/atlantic-sun/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(A-SUN).csv",
        "name_mapping": {
            'Dallas Howell ': 'Dallas Howell',
            'Cornelius Williams': 'Corneilous Williams',
            'Kamrin Oriol ': 'Kamrin Oriol',
            'George Kimblw III': 'George Kimble III',
            'Robert McCray': 'Robert McCray V',
            'Jevin Muniz ' : 'Jevin Muniz',
            'Ubongabasi Etim': 'Ubong Abasi Etim',
            'Jack Karasinki ': 'Jack Karasinski',
            'Jamie Phillips': 'Jamie Phillips Jr.'
        }
    },
    {
        "name": "American East",
        "url": "https://www.sports-reference.com/cbb/conferences/america-east/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(American East).csv",
        "name_mapping": {
            'Cam Morris': 'Cam Morris III',
            'AJ Lopez': 'A.J. Lopez',
            'Amare Marshall': 'Amar\'e Marshall',
            'Kheni Briggs ': 'Kheni Briggs',
            'Marcus Banks Jr. ': 'Marcus Banks Jr.',
            'Tim Moore Jr. ': 'Tim Moore'
        }
    },
    {
        "name": "Big Sky",
        "url": "https://www.sports-reference.com/cbb/conferences/big-sky/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Big Sky).csv",
        "name_mapping": {
            'Terri Miller': 'Terri Miller Jr.',
            'Mason Williams ': 'Mason Williams',
            'Sebastian Hartman': 'Sebastian Hartmann'
        }
    },
    {
        "name": "Big South",
        "url": "https://www.sports-reference.com/cbb/conferences/big-south/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Big South).csv",
        "name_mapping": {
            'RJ Johnson': 'R.J. Johnson',
            'Darryl Simmons II': 'Daryl Simmons II',
            'Pharrel Boyogueno': 'Pharell Boyogueno',
            'Kory Mincy ': 'Kory Mincy'
        }
    },
    {
        "name": "Big West",
        "url": "https://www.sports-reference.com/cbb/conferences/big-west/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Big West).csv",
        "name_mapping": {
            'Nordin Kapic ': 'Nordin Kapic',
            'Tyler McGhie ': 'Tyler McGhie',
            'Justin Rochelin ': 'Justin Rochelin',
            'Kam Martin ': 'Kam Martin',
            'Che Myles ': 'Myles Che',
            'Elijah Chol': 'Eli Chol',
            'Scotty Washington ': 'Scotty Washington',
            'Marcus Adams Jr': 'Marcus Adams Jr.',
            'Grady Lewis ': 'Grady Lewis',
            'Mahmoud Fofana ': 'Mahmoud Fofana',
            'Gytis Nemeiksa': 'Gytis Nemeikša',
            'Jason Fontenent II ': 'Jason Fontenet II',
            'Zion Sensley ': 'Zion Sensley',
            'Ben Shtolzberg ': 'Ben Shtolzberg',
            'Issac Jessup': 'Isaac Jessup',
            'Peter Bandeji': 'Peter Bandelj',
            'Kieran Elliot': 'Kieran Elliott',
            'Shakir Odunewu': 'Shakiru Odunewu',
            'Kaleb Brown ': 'Kaleb Brown'
        }
    },
    {
        "name": "Big East",
        "url": "https://www.sports-reference.com/cbb/conferences/big-east/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Big East).csv",
        "name_mapping": {
            'Fedor Zugic': 'Fedor Žugić',
            'Theo Pierre-Justin': 'Théo Pierre-Justin',
            'Casey O\'Mailey': 'Casey O\'Malley',
            'Oswin Erhunmqunse': 'Oswin Erhunmwunse',
            'Curtis Williams Jr': 'Curtis Williams',
            'Eli Edlaurier': 'Eli DeLaurier',
            'Ruben Prey': 'Rubén Prey',
            'Godswill Erherience': 'Godswill Erheriene'
        }
    },
    {
        "name": "CAA",
        "url": "https://www.sports-reference.com/cbb/conferences/coastal/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(CAA).csv",
        "name_mapping": {
            'George Beal Jr.': 'George Beale',
            'Kijan Robinson': 'KiJan Robinson',
            'Nolan Hodge ': 'Nolan Hodge',
            'Kobe Magee': 'Kobe MaGee',
            'Colby Dugggan': 'Colby Duggan',
            'Kyle Frazier ': 'Kyle Frazier',
            'Abdi Bashir': 'Abdi Bashir Jr.'
        }
    },
    {
        "name": "C-USA",
        "url": "https://www.sports-reference.com/cbb/conferences/cusa/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(C-USA).csv",
        "name_mapping": {
            'Brett Decker Jr. ': 'Brett Decker Jr.',
            'Ahmad Bynum': 'Ahamad Bynum',
            'David Terrel Jr.': 'David Terrell Jr.',
            'Tyrone Marshall Jr.': 'Tyrone Marshall'
        }
    },
    {
        "name": "Horizon",
        "url": "https://www.sports-reference.com/cbb/conferences/horizon/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Horizon).csv",
        "name_mapping": {
            'Chris Carrol': 'Cris Carroll',
            'Corey Hadnot': 'Corey Hadnot II',
            'Keeyan Itijere': 'Keeyan Itejere',
            'Allen Mukeba': 'Allen David Mukeba',
            'Ryan Prather': 'Ryan Prather Jr.',
            'DJ Smith': 'D.J. Smith'
        }
    },
    {
        "name": "Ivy",
        "url": "https://www.sports-reference.com/cbb/conferences/ivy/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Ivy).csv",
        "name_mapping": {
            'Zine Eddine Bedri': 'Zinou Eddine Bedri',
            'Alex Lesburt Jr.': 'AJ Lesburt Jr.',
            'Casey Simmons ': 'Casey Simmons',
            'Connor Amundson': 'Connor Amundsen',
            'Thomas Batties': 'Thomas Batties II'
        }
    },
    {
        "name": "MAAC",
        "url": "https://www.sports-reference.com/cbb/conferences/maac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(MAAC).csv",
        "name_mapping": {
            'Jadin Collins-Roberts': 'Jadin Collins',
            'Amarri Monroe': 'Amarri Tice',
            'Adam Clarke': 'Adam Clark',
            'Shaqil Bender': 'Shaquil Bender',
            'Adam Njie': 'Adam Njie Jr.'
        }
    },
    {
        "name": "MAC",
        "url": "https://www.sports-reference.com/cbb/conferences/mac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(MAC).csv",
        "name_mapping": {
            'Delrecco Gillespie ': 'Delrecco Gillespie',
            'Marvin Musiime-Kamali ': 'Marvin Musiime-Kamali',
            'Antwone Woofolk': 'Antwone Woolfolk',
            'Javan Simmons ': 'Javan Simmons',
            'Trey Pettigrew ': 'Trey Pettigrew',
            'Jackson Paveletzke ': 'Jackson Paveletzke',
            'Ben Estis ': 'Ben Estis',
            'Juanse Gorosito': 'Juan Sebastian Gorosito',
            'TJ Burch ': 'TJ Burch',
            'Zane Doughty `': 'Zane Doughty',
            'Jurica Zagorsak ': 'Jurica Zagorsak',
            'Jakobi Heady (Older Brother)': 'Jakobi Heady',
            'Ugnius Jarusevicius ': 'Ugnius Jarusevicius',
            'Quentin Heady (Younger Brother)': 'Quentin Heady',
            'Hunter Harding ': 'Hunter Harding',
            'Youssef Khayat ': 'Youssef Khayat',
            'Ben Michaels ': 'Ben Michaels',
            'Noah Batchelor ': 'Noah Batchelor',
            'Markhi Strickland ': 'Markhi Strickland',
            'Mehki Cooper': 'Mekhi Cooper',
            'Wilguens Jr. Exacte ': 'Wilguens Jr. Exacte'
        }
    },
    {
        "name": "Missouri Valley",
        "url": "https://www.sports-reference.com/cbb/conferences/mvc/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Missouri Valley).csv",
        "name_mapping": {
            'Markus Harding ': 'Markus Harding',
            'Cam Manyawu ': 'Cam Manyawu',
            'Tyler Lundblade ': 'Tyler Lundblade',
            'Kyeron Lindsay': 'KyeRon Lindsay',
            'Javon Jackson ': 'Javon Jackson',
            'Sasa Ciani': 'Saša Ciani',
            'Vincent Brady': 'Vincent Brady II',
            'Jefferson De La Cruz Monegro': 'Jefferson Monegro',
            'Christian Davis ': 'Christian Davis',
            'All Wright ': 'All Wright',
            'Cam Haffner': 'Cameron Haffner',
            'Kennard Davis Jr.': 'Kennard Davis'
        }
    },
    {
        "name": "MEAC",
        "url": "https://www.sports-reference.com/cbb/conferences/meac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(MEAC).csv",
        "name_mapping": {
            'Drayton Jones ': 'Drayton Jones',
            'Mitchell Taylor ': 'Mitchell Taylor',
            'Harper Blake': 'Blake Harper',
            'Kiran Oliver ': 'Kiran Oliver',
            'Demajion Topps ': 'Demajion Topps',
            'Po\'Boigh King ': 'Po\'Boigh King',
            'Perry Smith Jr. ': 'Perry Smith Jr.',
            'KC Shaw (Older Brother)': 'Ketron Shaw',
            'Cardell Bailey ': 'Cardell Bailey',
            'Kyrell Shaw (Younger Brother)': 'Kyrell Shaw',
            'Toby Nnadozie ': 'Toby Nnadozie',
            'Camarren Sparrow ': 'Camaren Sparrow',
            'Jaqai Murray ': 'Jaqai Murray',
            'Jayden Johnson ': 'Jayden Johnson'
        }
    },
    {
        "name": "NEC",
        "url": "https://www.sports-reference.com/cbb/conferences/northeast/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(NEC).csv",
        "name_mapping": {
            'Bobby Rosenberger III': 'Bobby Rosenberger',
            'Ace Talbert': 'Aaron Talbert',
            'Terrence Brown ': 'Terrence Brown'
        }
    },
    {
        "name": "OVC",
        "url": "https://www.sports-reference.com/cbb/conferences/ovc/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(OVC).csv",
        "name_mapping": {
            'Teddy Washington Jr. ': 'Tedrick Washington',
            'Josue Grullon': 'Josué Grullon',
            'Brian Taylor II': 'Brian Taylor'
        }
    },
    {
        "name": "Patriot",
        "url": "https://www.sports-reference.com/cbb/conferences/patriot/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Patriot).csv",
        "name_mapping": {
            'Milos Ilic ': 'Milos Ilic'
        }
    },
    {
        "name": "SoCon",
        "url": "https://www.sports-reference.com/cbb/conferences/southern/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(SoCon).csv",
        "name_mapping": {
            'Tom House ': 'Tom House',
            'Ice Emery': 'Chevalier Emery'
        }
    },
    {
        "name": "Southland",
        "url": "https://www.sports-reference.com/cbb/conferences/southland/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Southland).csv",
        "name_mapping": {
            'Robert Brown': 'Rob Brown III',
            'Jon Sanders II': 'Jon Sanders',
            'DJ Richards ': 'DJ Richards',
            'T\'john Brown': 'T\'Johnn Brown',
            'Hasan Abdul Hakim': 'Hasan Abdul-Hakim',
            'KT Raimey': 'K.T. Raimey',
            'Howe Fleming Jr.': 'Howard Fleming Jr.',
            'Bryson Hawkins': 'Bryson Dawkins',
            'Matt Hayman': 'Kyle Hayman'
        }
    },
    {
        "name": "SWAC",
        "url": "https://www.sports-reference.com/cbb/conferences/swac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(SWAC).csv",
        "name_mapping": {
            'Trey Thomas': 'Tre Thomas',
            'Raphael Dumont': 'Raphael Dumon',
            'Quentin Bolton': 'Quentin Bolton Jr.',
            'Kenny Hunter ': 'Kenny Hunter',
            'Chilaydrein Newton': 'Chilaydrien Newton',
            'Jayme Mitchell Jr.': 'Jayme Mitchell',
            'Reggie Ward Jr. ': 'Reggie Ward Jr.'
        }
    },
    {
        "name": "Summit",
        "url": "https://www.sports-reference.com/cbb/conferences/summit/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Summit).csv",
        "name_mapping": {
            'Noah Fedderson': 'Noah Feddersen',
            'Nolan Minessale ': 'Nolan Minessale',
            'Chase Forte ': 'Chase Forte',
            'Issac Bruns': 'Isaac Bruns',
            'Nicholas Shogbonyo ': 'Nicholas Shogbonyo',
            'Deandre Craig': 'DeAndre Craig',
            'Mier Panoam ': 'Mier Panoam',
            'Lance Waddles ': 'Lance Waddles',
            'Darius Robinson Jr. ': 'Darius Robinson Jr.',
            'Kasheem Grady': 'Kasheem Grady II',
            'Amar Kulijuhovic ': 'Amar Kuljuhovic'
        }
    },
    {
        "name": "Sun Belt",
        "url": "https://www.sports-reference.com/cbb/conferences/sun-belt/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Sun Belt).csv",
        "name_mapping": {
            'Pinio Joseph ': 'Joseph Pinion',
            'Barry Dunning Jr': 'Barry Dunning Jr.',
            'Joshua O\'Garro ': 'Josh O\'Garro',
            'Robert Davis jr.': 'Robert Davis Jr.',
            'Devin Ceasaer': 'Devin Ceaser',
            'Jaden johnson': 'Jaden Johnson',
            'Adante\' Holiman': 'Adante’ Holiman',
            'Bryce Lindsay ': 'Bryce Lindsay',
            'Ryan Nutter ': 'Ryan Nutter',
            'Rsheed Jones ': 'RaSheed Jones',
            'Joshua Meo': 'Josh Meo',
            'Jalil Bearburn ': 'Jalil Bearburn',
            'Dior Conners ': 'Dior Conners',
            'Jaylen Bolden': 'Jalen Bolden',
            'Junior Wilson': 'Jacob Wilson',
            'Mostapha el Moutaouakkil': 'Mostapha El Moutaouakkil',
            'Jalil Bearburn ': 'Jalil Beaubrun'
        }
    },
    {
        "name": "WAC",
        "url": "https://www.sports-reference.com/cbb/conferences/wac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(WAC).csv",
        "name_mapping": {
            'MAKAIH WILLIAMS': 'Makaih Williams',
            'CALEB SHAW': 'Caleb Shaw',
            'STYLES PHIPPS': 'Styles Phipps',
            'SAMMIE YEANAY': 'Sammie Yeanay',
            'TAVI JACKSON': 'Taviontae Jackson',
            'BROCK FELDER': 'Brock Felder',
            'JAMIR SIMPSON': 'Jamir Simpson',
            'DOMINIQUE FORD': 'Dominique Ford',
            'DOMINICK NELSON': 'Dominick Nelson',
            'CARTER WELLING': 'Carter Welling',
            'TANNER TOOLSON': 'Tanner Toolson',
            'ETHAN POTTER': 'Ethan Potter',
            'TREVAN LEONHARDT': 'Trevan Leonhardt',
            'LEONARDO BETTIOL': 'Leonardo Bettiol',
            'QUION WILLIAMS': 'Quion Williams',
            'RAYSEAN SEAMSTER': 'Raysean Seamster',
            'JAXON ELLINGSWORTH': 'Jaxon Ellingsworth',
            'DOMINIQUE DANIELS JR. ': 'Dominique Daniels Jr.',
            'MARTEL WILLIAMS': 'Martel Williams',
            'BUBU BENJAMIN': 'Bubu Benjamin',
            'KEITENN BRISTOW': 'Keitenn Bristow',
            'RONNIE HARRISON JR. ': 'Ronnie Harrison',
            'DANTWAN GRIMES': 'Dantwan Grimes',
            'JOHN CHRISTOFILIS': 'John Christofilis',
            'MALEEK ARINGTON': 'Maleek Arington',
            'BRAYDEN MALDONADO': 'Brayden Maldonado',
            'MADIBA OWONA': 'Madiba Owona',
            'SAMUEL ARIYIBI': 'Samuel Ariyibi'
        }
    },
    {
        "name": "A10",
        "url": "https://www.sports-reference.com/cbb/conferences/atlantic-10/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(A10).csv",
        "name_mapping": {
            'Robert Blums': 'Roberts Blums',
            'Amael L\'etang': 'Amael L\'Etang',
            'Malek Adbelgowad': 'Malek Abdelgowad',
            'Tyronne Farrell': 'Tyonne Farrell',
            'Larry Hughes II': 'Larry Hughes Jr.'
        }
    },
    {
        "name": "AAC",
        "url": "https://www.sports-reference.com/cbb/conferences/aac/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(AAC).csv",
        "name_mapping": {
            'Kaleb Banks ': 'Kaleb Banks',
            'Greg Glen II': 'Gregg Glenn III',
            'Damarien Yates': 'Demarien Yates',
            'Babatunde Durodola ': 'Babatunde Durodola',
            'CJ Brown ': 'CJ Brown',
            'Yaxel Lendeborg ': 'Yaxel Lendeborg',
            'Yann Ferell': 'Yann Farell',
            'Tre Carrol': 'Tre Carroll',
            'Baba Miller ': 'Baba Miller',
            'Leland Walker ': 'Leland Walker',
            'Damari Monsato': 'Damari Monsanto',
            'Aleks Szymczyk': 'Aleksander Szymczyk',
            'Ian Smikle ': 'Ian Smikle',
            'Jyaden Reid': 'Jayden Reid',
            'Matt Reed': 'Matthew Reed'
        }
    },
    {
        "name": "Mountain West",
        "url": "https://www.sports-reference.com/cbb/conferences/mwc/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(Mountain West).csv",
        "name_mapping": {
            'Flip Borovicanin': 'Filip Borvicanin',
            'Jalen Bedford': 'Jailen Bedford'
        }
    },
    {
        "name": "WCC",
        "url": "https://www.sports-reference.com/cbb/conferences/wcc/men/2025-stats.html",
        "given_file": "sample_DB/given_data/Portal Recon(WCC).csv",
        "name_mapping": {
            'Andrew McKeever ': 'Andrew McKeever',
            'Ryan Beasley ': 'Ryan Beasley',
            'Liutauras Lelevicius ': 'Liutauras Lelevicius',
            'JQ Williford': 'Ja\'Quavis Williford',
            'Christopher Tilly': 'Christoph Tilly',
            'Steven Jamerson': 'Steven Jamerson II',
            'Kenyon Kensie Jr.': 'Keyon Kensie',
            'Jevon Porter ': 'Jevon Porter',
            'Myron Amey Jr.': 'MJ Amey Jr.',
            'Aaron McBride ': 'Aaron McBride',
            'Jaxon Olvera ': 'Jaxon Olvera',
            'Jermaine Ballisager Webb': 'Jermaine Ballisager-Webb',
            'Lamar Washington ': 'Lamar Washington',
            'Jazz Gardner ': 'Jazz Gardner'
        }
    }
]

# Column order
column_order = [
    'Player Name', 'Pos', 'School', 'Year', 'Height', 'Weight', 'Notes',
    'Hometown', 'High School', 'AAU Team', 'G', 'GS', 'MP/G', 'PTS/G', 
    'RB/G', 'AST/G', 'STL/G', 'BLK/G', 'TOV/G', 'FG/G', 'FGA/G', 
    'FG%', '3P/G', '3PA/G', '3P%', '2P/G', '2PA/G', '2P%', 
    'eFG%', 'FT/G', 'FTA/G', 'FT%', 'ORB/G', 'DRB/G', 'PF/G'
]

# Process each conference
for conf in conferences:
    success = False
    retries = 3

    for attempt in range(retries):
        response = requests.get(conf["url"])
        response.encoding = 'utf-8'
        
        # Check for successful response
        if response.status_code == 200:
            success = True
            break
        elif response.status_code == 429:
            print(f"{conf['name']}: Rate limit hit. Retrying in {2 ** attempt} seconds...")
            time.sleep(2 ** attempt)
        else:
            print(f"{conf['name']}: Failed to retrieve the webpage. Status code: {response.status_code}")
            break

    if not success:
        print(f"{conf['name']}: Skipping due to repeated errors.")
        continue

    print(f"{conf['name']}: Successful response.")

    # Store HTML content in memory
    soup = BeautifulSoup(response.text, 'html.parser')
    # Find the table
    table = soup.find('table', id='players_per_game')

    if table:
        # Convert HTML Table to a DataFrame
        df = pd.read_html(StringIO(str(table)), header=0)[0]

        # Import given data
        given_df = pd.read_csv(conf["given_file"])

        # Save raw data
        raw_file_path = os.path.join(raw_save_dir, f"raw_{conf['name'].lower().replace(' ', '_')}.csv")
        df.to_csv(raw_file_path, index=False)

        # Clean data
        given_df = given_clean(given_df)
        df = bb_ref_clean(df)
        # Match names correctly
        given_df['Player Name'] = given_df['Player Name'].replace(conf["name_mapping"])

        # Left join the given list of players with basketball reference data
        results = pd.merge(given_df, df, left_on='Player Name', right_on='Player', how='left')
        results = results.drop(columns=['Player'])
        results = results[column_order]

        # Sort by minutes per game (MP/G) in descending order
        if 'MP/G' in results.columns:
            results = results.sort_values(by='MP/G', ascending=False)

        # Save cleaned data
        final_file_path = os.path.join(clean_save_dir, f"{conf['name'].lower().replace(' ', '_')}.csv")
        results.to_csv(final_file_path, index=False)

        print(f"{conf['name']}: Data cleaned and saved.")
    else:
        print(f"{conf['name']}: Table not found.")

    # Delay to avoid hitting rate limits
    time.sleep(2)  # Adjust delay as needed

# combine all of the .csv files into a .xlsx file
combine_csv_to_excel(clean_save_dir, output_excel_file)

