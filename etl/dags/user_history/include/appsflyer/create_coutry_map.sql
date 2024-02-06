-- Description: This script creates a table with country codes and country names
--              for the countries that are not present in the table
--              gametuner_common.appsflyer_country_codes_map or on first run.
--              This table is used to map country codes to country names in the
--              appsflyer data.

CREATE TABLE IF NOT EXISTS `{project_id}.gametuner_common.appsflyer_country_codes_map` (
  geo_country STRING,
  geo_country_name STRING
);

CREATE OR REPLACE TEMP TABLE `appsflyer_country_codes_map_temp` (
  geo_country STRING,
  geo_country_name STRING
);

INSERT INTO `appsflyer_country_codes_map_temp` (geo_country, geo_country_name)
VALUES
    ('UK', 'United Kingdom'),
    ('CU', 'Cuba'),
    ('EU', 'European Union'),
    ('SO', 'Somalia'),
    ('CL', 'Chile'),
    ('LU', 'Luxembourg'),
    ('GR', 'Greece'),
    ('TD', 'Chad'),
    ('CI', 'Ivory Coast'),
    ('NI', 'Nicaragua'),
    ('MN', 'Mongolia'),
    ('JE', 'Jersey'),
    ('NO', 'Norway'),
    ('PF', 'French Polynesia'),
    ('PW', 'Palau'),
    ('UZ', 'Uzbekistan'),
    ('LV', 'Latvia'),
    ('SK', 'Slovakia'),
    ('KN', 'St Kitts and Nevis'),
    ('GG', 'Guernsey'),
    ('MS', 'Montserrat'),
    ('VN', 'Vietnam'),
    ('TN', 'Tunisia'),
    ('HT', 'Haiti'),
    ('ST', 'São Tomé and Príncipe'),
    ('BZ', 'Belize'),
    ('KM', 'Comoros'),
    ('TC', 'Turks and Caicos Islands'),
    ('MC', 'Monaco'),
    ('NF', 'Norfolk Island'),
    ('TH', 'Thailand'),
    ('JM', 'Jamaica'),
    ('DJ', 'Djibouti'),
    ('TZ', 'Tanzania'),
    ('DZ', 'Algeria'),
    ('ZW', 'Zimbabwe'),
    ('MD', 'Moldova'),
    ('XK', 'Kosovo'),
    ('EH', 'Western Sahara'),
    ('CK', 'Cook Islands'),
    ('SC', 'Seychelles'),
    ('SM', 'San Marino'),
    ('NU', 'Niue'),
    ('PL', 'Poland'),
    ('IR', 'Iran'),
    ('BO', 'Bolivia'),
    ('SG', 'Singapore'),
    ('AD', 'Andorra'),
    ('GY', 'Guyana'),
    ('ET', 'Ethiopia'),
    ('GI', 'Gibraltar'),
    ('MQ', 'Martinique'),
    ('IL', 'Israel'),
    ('FO', 'Faroe Islands'),
    ('BI', 'Burundi'),
    ('BJ', 'Benin'),
    ('CH', 'Switzerland'),
    ('NG', 'Nigeria'),
    ('MR', 'Mauritania'),
    ('FK', 'Falkland Islands'),
    ('AU', 'Australia'),
    ('KE', 'Kenya'),
    ('CF', 'Central African Republic'),
    ('IE', 'Ireland'),
    ('NA', 'Namibia'),
    ('HK', 'Hong Kong'),
    ('VC', 'St Vincent and Grenadines'),
    ('SD', 'Sudan'),
    ('PM', 'Saint Pierre and Miquelon'),
    ('SZ', 'Eswatini'),
    ('BT', 'Bhutan'),
    ('AI', 'Anguilla'),
    ('VI', 'U.S. Virgin Islands'),
    ('VU', 'Vanuatu'),
    ('YE', 'Yemen'),
    ('GP', 'Guadeloupe'),
    ('SL', 'Sierra Leone'),
    ('DE', 'Germany'),
    ('IM', 'Isle of Man'),
    ('BL', 'Saint Barthélemy'),
    ('GF', 'French Guiana'),
    ('BH', 'Bahrain'),
    ('BE', 'Belgium'),
    ('NR', 'Nauru'),
    ('CR', 'Costa Rica'),
    ('MX', 'Mexico'),
    ('CG', 'Congo Republic'),
    ('CV', 'Cabo Verde'),
    ('GM', 'Gambia'),
    ('AF', 'Afghanistan'),
    ('DM', 'Dominica'),
    ('GT', 'Guatemala'),
    ('CD', 'DR Congo'),
    ('SS', 'South Sudan'),
    ('BM', 'Bermuda'),
    ('SY', 'Syria'),
    ('AQ', 'Antarctica'),
    ('TR', 'Turkey'),
    ('ES', 'Spain'),
    ('CN', 'China'),
    ('GU', 'Guam'),
    ('SX', 'Sint Maarten'),
    ('BN', 'Brunei'),
    ('LR', 'Liberia'),
    ('HU', 'Hungary'),
    ('UA', 'Ukraine'),
    ('TM', 'Turkmenistan'),
    ('AT', 'Austria'),
    ('TG', 'Togo'),
    ('MG', 'Madagascar'),
    ('SB', 'Solomon Islands'),
    ('ID', 'Indonesia'),
    ('NL', 'Netherlands'),
    ('PR', 'Puerto Rico'),
    ('SA', 'Saudi Arabia'),
    ('GA', 'Gabon'),
    ('TJ', 'Tajikistan'),
    ('MF', 'Saint Martin'),
    ('CZ', 'Czechia'),
    ('PA', 'Panama'),
    ('IT', 'Italy'),
    ('VE', 'Venezuela'),
    ('BQ', 'Bonaire, Sint Eustatius, and Saba'),
    ('LT', 'Lithuania'),
    ('VG', 'British Virgin Islands'),
    ('MA', 'Morocco'),
    ('MV', 'Maldives'),
    ('TV', 'Tuvalu'),
    ('RW', 'Rwanda'),
    ('MO', 'Macao'),
    ('ER', 'Eritrea'),
    ('MT', 'Malta'),
    ('AM', 'Armenia'),
    ('PG', 'Papua New Guinea'),
    ('SN', 'Senegal'),
    ('EE', 'Estonia'),
    ('IO', 'British Indian Ocean Territory'),
    ('AL', 'Albania'),
    ('BR', 'Brazil'),
    ('NP', 'Nepal'),
    ('MZ', 'Mozambique'),
    ('EG', 'Egypt'),
    ('RS', 'Serbia'),
    ('MP', 'Northern Mariana Islands'),
    ('BY', 'Belarus'),
    ('LB', 'Lebanon'),
    ('BA', 'Bosnia and Herzegovina'),
    ('GE', 'Georgia'),
    ('BS', 'Bahamas'),
    ('PS', 'Palestine'),
    ('KR', 'South Korea'),
    ('IS', 'Iceland'),
    ('MW', 'Malawi'),
    ('WS', 'Samoa'),
    ('GQ', 'Equatorial Guinea'),
    ('PK', 'Pakistan'),
    ('DO', 'Dominican Republic'),
    ('SI', 'Slovenia'),
    ('PY', 'Paraguay'),
    ('IN', 'India'),
    ('CO', 'Colombia'),
    ('HN', 'Honduras'),
    ('FI', 'Finland'),
    ('KI', 'Kiribati'),
    ('AR', 'Argentina'),
    ('CM', 'Cameroon'),
    ('LK', 'Sri Lanka'),
    ('BW', 'Botswana'),
    ('IQ', 'Iraq'),
    ('RU', 'Russia'),
    ('ME', 'Montenegro'),
    ('MK', 'North Macedonia'),
    ('GN', 'Guinea'),
    ('PT', 'Portugal'),
    ('UY', 'Uruguay'),
    ('BG', 'Bulgaria'),
    ('TT', 'Trinidad and Tobago'),
    ('MY', 'Malaysia'),
    ('KZ', 'Kazakhstan'),
    ('AO', 'Angola'),
    ('LC', 'Saint Lucia'),
    ('SH', 'Saint Helena'),
    ('UG', 'Uganda'),
    ('WF', 'Wallis and Futuna'),
    ('GL', 'Greenland'),
    ('NE', 'Niger'),
    ('JP', 'Japan'),
    ('KG', 'Kyrgyzstan'),
    ('BB', 'Barbados'),
    ('CY', 'Cyprus'),
    ('AX', 'Åland Islands'),
    ('GW', 'Guinea-Bissau'),
    ('CW', 'Curaçao'),
    ('ML', 'Mali'),
    ('LA', 'Laos'),
    ('BF', 'Burkina Faso'),
    ('EC', 'Ecuador'),
    ('MM', 'Myanmar'),
    ('NC', 'New Caledonia'),
    ('KW', 'Kuwait'),
    ('AS', 'American Samoa'),
    ('PH', 'Philippines'),
    ('TL', 'Timor-Leste'),
    ('LS', 'Lesotho'),
    ('GH', 'Ghana'),
    ('HR', 'Croatia'),
    ('PE', 'Peru'),
    ('GD', 'Grenada'),
    ('AZ', 'Azerbaijan'),
    ('SV', 'El Salvador'),
    ('ZA', 'South Africa'),
    ('SE', 'Sweden'),
    ('LY', 'Libya'),
    ('OM', 'Oman'),
    ('FJ', 'Fiji'),
    ('ZM', 'Zambia'),
    ('RO', 'Romania'),
    ('AE', 'United Arab Emirates'),
    ('LI', 'Liechtenstein'),
    ('FR', 'France'),
    ('KY', 'Cayman Islands'),
    ('AW', 'Aruba'),
    ('YT', 'Mayotte'),
    ('DK', 'Denmark'),
    ('MU', 'Mauritius'),
    ('MH', 'Marshall Islands'),
    ('FM', 'Federated States of Micronesia'),
    ('RE', 'Réunion'),
    ('SR', 'Suriname'),
    ('TW', 'Taiwan'),
    ('TO', 'Tonga'),
    ('JO', 'Jordan'),
    ('QA', 'Qatar'),
    ('US', 'United States'),
    ('BD', 'Bangladesh'),
    ('CA', 'Canada'),
    ('AG', 'Antigua and Barbuda'),
    ('NZ', 'New Zealand'),
    ('KH', 'Cambodia');

INSERT INTO `{project_id}.gametuner_common.appsflyer_country_codes_map` (geo_country, geo_country_name)
SELECT * FROM `appsflyer_country_codes_map_temp` AS new_countries
WHERE NOT EXISTS (
  SELECT 1 FROM `{project_id}.gametuner_common.appsflyer_country_codes_map`
  WHERE geo_country = new_countries.geo_country
);