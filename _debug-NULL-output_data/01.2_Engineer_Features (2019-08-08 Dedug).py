# Databricks notebook source
import uuid
import warnings

import acosta
from acosta.alerting.preprocessing import pos_to_training_data

print(acosta.__version__)

# COMMAND ----------

# week_seasonality_pandas_map = {
#     0: -0.004611370903131311,
#     1: -0.07252493164242498,
#     2: -0.052317387136431436,
#     3: -0.0592789875354809,
#     4: 0.09643529118688565,
#     5: 0.24181872679285424,
#     6: 0.22998183116902732
# }

# week_seasonality_spark_map = {
#     2: -0.004611370903131311,  # Monday
#     3: -0.07252493164242498,  # Tuesday
#     4: -0.052317387136431436,  # Wednesday
#     5: -0.0592789875354809,  # Thursday
#     6: 0.09643529118688565,  # Friday
#     7: 0.24181872679285424,  # Saturday
#     1: 0.22998183116902732  # Sunday
# }

# COMMAND ----------

# year_seasonality_map = {
#     1: -0.12825029017474596,
#     2: 0.03209255590608536,
#     3: 0.12667272710359875,
#     4: 0.16914991848749567,
#     5: 0.1731838251274779,
#     6: 0.15243414209324696,
#     7: 0.12056056445450461,
#     8: 0.09122278728095239,
#     9: 0.07753146037229797,
#     10: 0.08254844705038843,
#     11: 0.1005399796108428,
#     12: 0.12547527088909116,
#     13: 0.15132353372056384,
#     14: 0.17205398094069094,
#     15: 0.1816358253849027,
#     16: 0.1740402979243215,
#     17: 0.14577270106513648,
#     18: 0.10082316711859655,
#     19: 0.04455706321515481,
#     20: -0.017660243514736208,
#     21: -0.08046338594062342,
#     22: -0.1384869969320543,
#     23: -0.1863657093585756,
#     24: -0.21910838944365676,
#     25: -0.23629639589356738,
#     26: -0.24057005221096112,
#     27: -0.23462579177604925,
#     28: -0.221160047969043,
#     29: -0.20286925417015375,
#     30: -0.18244984375959283,
#     31: -0.16259469636329105,
#     32: -0.14528104012390558,
#     33: -0.1309075397476674,
#     34: -0.11966205479063634,
#     35: -0.1117324448088723,
#     36: -0.1073065693584351,
#     37: -0.10657228799538457,
#     38: -0.10971746027578058,
#     39: -0.11684317107995239,
#     40: -0.1273079636313205,
#     41: -0.14009337651165804,
#     42: -0.15417798803109278,
#     43: -0.1685403764997523,
#     44: -0.18215912022776407,
#     45: -0.19401279752525574,
#     46: -0.2030859658829964,
#     47: -0.20880368275958006,
#     48: -0.2113254633610671,
#     49: -0.21088045421675738,
#     50: -0.20769780185595024,
#     51: -0.2020066528079452,
#     52: -0.19403615360204168,
#     53: -0.18401545076753933,
#     54: -0.1721916232732529,
#     55: -0.15892241648556119,
#     56: -0.14460775021192568,
#     57: -0.12964762728036058,
#     58: -0.11444205051888064,
#     59: -0.09939102275550016,
#     60: -0.08489454681823388,
#     61: -0.07135352957813672,
#     62: -0.059202385018499046,
#     63: -0.04891812969887217,
#     64: -0.04098053626702113,
#     65: -0.035869377370711275,
#     66: -0.03406442565770776,
#     67: -0.03604545377577583,
#     68: -0.042292234372680704,
#     69: -0.05294232606256431,
#     70: -0.066585939330538,
#     71: -0.08137615528840408,
#     72: -0.09546602867893036,
#     73: -0.1070086142448843,
#     74: -0.11415696672903372,
#     75: -0.11506414087414626,
#     76: -0.10798348201410765,
#     77: -0.09336814300602589,
#     78: -0.07381329759395404,
#     79: -0.052002850218368105,
#     80: -0.030620705319744715,
#     81: -0.01235076733855993,
#     82: 0.0001230592847100108,
#     83: 0.004117072412363923,
#     84: -0.0019997063925805605,
#     85: -0.01635144185627969,
#     86: -0.03633298970750095,
#     87: -0.05933920567501197,
#     88: -0.08276494548757979,
#     89: -0.10400506487397225,
#     90: -0.12045441956295636,
#     91: -0.12964896991240824,
#     92: -0.13113692758713996,
#     93: -0.12596637329402288,
#     94: -0.1152212335418195,
#     95: -0.09998543483929213,
#     96: -0.08134290369520315,
#     97: -0.060377566618314955,
#     98: -0.03817376958427613,
#     99: -0.01596590400425171,
#     100: 0.004640249938606062,
#     101: 0.021982447066407413,
#     102: 0.034398442201262557,
#     103: 0.040225990165281764,
#     104: 0.03780284578057499,
#     105: 0.02546676386925266,
#     106: 0.0018704257469637571,
#     107: -0.03123964939811761,
#     108: -0.07036091224984597,
#     109: -0.1119706581964869,
#     110: -0.1525461826263083,
#     111: -0.18856478092757747,
#     112: -0.21650374848856194,
#     113: -0.2328601332758206,
#     114: -0.23619470961976166,
#     115: -0.22891178711380053,
#     116: -0.21383514567126044,
#     117: -0.19378856520546442,
#     118: -0.17159582562973652,
#     119: -0.15008070685739985,
#     120: -0.1320669888017776,
#     121: -0.12001472591936113,
#     122: -0.11383238743807207,
#     123: -0.11233615230612473,
#     124: -0.11433805641145114,
#     125: -0.11865013564198368,
#     126: -0.12408442588565458,
#     127: -0.1294529630303962,
#     128: -0.13358206290069297,
#     129: -0.13597735445501366,
#     130: -0.1371063447649048,
#     131: -0.13750948561896198,
#     132: -0.13772722880578092,
#     133: -0.13830002611395717,
#     134: -0.1397683293320864,
#     135: -0.14267259024876433,
#     136: -0.1474247332600371,
#     137: -0.15377929388701034,
#     138: -0.16128118493904076,
#     139: -0.16947522895658979,
#     140: -0.17790624848011857,
#     141: -0.18611906605008846,
#     142: -0.19365850420696085,
#     143: -0.20007730818323943,
#     144: -0.20513957747979783,
#     145: -0.20883834138826965,
#     146: -0.21117807402936237,
#     147: -0.21216324952378365,
#     148: -0.21179834199224104,
#     149: -0.21008782555544217,
#     150: -0.20703617481516554,
#     151: -0.20270799631649256,
#     152: -0.1973941466732667,
#     153: -0.19143868509680503,
#     154: -0.18518567079842402,
#     155: -0.17897916298944044,
#     156: -0.17316322088117111,
#     157: -0.16808190368493253,
#     158: -0.16407310253174395,
#     159: -0.16137110407887076,
#     160: -0.16012422736442733,
#     161: -0.16047818926765223,
#     162: -0.1625787066677839,
#     163: -0.16657149644406108,
#     164: -0.17260227547572218,
#     165: -0.1808164714470828,
#     166: -0.1911379150938113,
#     167: -0.20287367542357151,
#     168: -0.2152243722739043,
#     169: -0.22739062548235142,
#     170: -0.23857305488645422,
#     171: -0.24797228032375418,
#     172: -0.25478892163179273,
#     173: -0.2582990275724107,
#     174: -0.2586342090891283,
#     175: -0.2564681045857447,
#     176: -0.2524829496465119,
#     177: -0.24736097985568187,
#     178: -0.24178443079750656,
#     179: -0.23643553805623801,
#     180: -0.2319949506801219,
#     181: -0.2288921944569727,
#     182: -0.22703349456998756,
#     183: -0.22625966943772838,
#     184: -0.2264115374787565,
#     185: -0.2273299171116337,
#     186: -0.22885562675492144,
#     187: -0.23082948482718135,
#     188: -0.23309813365703563,
#     189: -0.2355548109929058,
#     190: -0.23811511495053964,
#     191: -0.24069478491307597,
#     192: -0.24320956026365367,
#     193: -0.2455751803854117,
#     194: -0.24770738466148895,
#     195: -0.24952359700639265,
#     196: -0.25104744618451186,
#     197: -0.2524701997972474,
#     198: -0.25399790498794056,
#     199: -0.25583660889993237,
#     200: -0.258192358676564,
#     201: -0.2612712014611765,
#     202: -0.26527918439711107,
#     203: -0.2703076846502193,
#     204: -0.2757836866683486,
#     205: -0.28089546456385006,
#     206: -0.2848309809203399,
#     207: -0.28677819832143464,
#     208: -0.28592507935075057,
#     209: -0.28145958659190373,
#     210: -0.2725985283592976,
#     211: -0.25951169044370587,
#     212: -0.24351734606538467,
#     213: -0.22600214351015913,
#     214: -0.20835273106385385,
#     215: -0.19195575701229545,
#     216: -0.17819786964130854,
#     217: -0.16846571723671835,
#     218: -0.16372069440810896,
#     219: -0.16311575636434317,
#     220: -0.16532321768462285,
#     221: -0.16901538910290215,
#     222: -0.17286458135313493,
#     223: -0.17554310516927524,
#     224: -0.1757232712852769,
#     225: -0.17215193062703643,
#     226: -0.16506695425139353,
#     227: -0.15608276393780537,
#     228: -0.14686538197297588,
#     229: -0.13908083064360877,
#     230: -0.1343951322364078,
#     231: -0.13447430903807678,
#     232: -0.1409839897015302,
#     233: -0.15467570526405058,
#     234: -0.17343432409018403,
#     235: -0.1945842476843136,
#     236: -0.21544987755082198,
#     237: -0.2333556151940918,
#     238: -0.2456258621185058,
#     239: -0.24958501982844666,
#     240: -0.24280558968568647,
#     241: -0.2261316166583698,
#     242: -0.20271762820716724,
#     243: -0.17576672076889158,
#     244: -0.14848199078035917,
#     245: -0.1240665346783833,
#     246: -0.10572344889977783,
#     247: -0.09664987315771988,
#     248: -0.09847249660824127,
#     249: -0.10914944290033515,
#     250: -0.12611588600840587,
#     251: -0.14680699990685772,
#     252: -0.16865795857009488,
#     253: -0.18910393597252256,
#     254: -0.20558010608854319,
#     255: -0.21580327042375574,
#     256: -0.22007073569665314,
#     257: -0.22006537068656087,
#     258: -0.2174833058258827,
#     259: -0.21402067154702273,
#     260: -0.2113735982823848,
#     261: -0.2112382164643729,
#     262: -0.21529681166938885,
#     263: -0.22402543639465763,
#     264: -0.23577495774109083,
#     265: -0.24867991693456895,
#     266: -0.2608748552009726,
#     267: -0.2704943137661822,
#     268: -0.27567283385607816,
#     269: -0.27454495669654094,
#     270: -0.2656499460384928,
#     271: -0.25018905829346527,
#     272: -0.2304390262422936,
#     273: -0.2086795892665985,
#     274: -0.187190486748004,
#     275: -0.1682514580681309,
#     276: -0.1541422426086012,
#     277: -0.14710242072714258,
#     278: -0.14769122882149757,
#     279: -0.1542135235837382,
#     280: -0.16481607836359138,
#     281: -0.17764566651078437,
#     282: -0.19084906137504423,
#     283: -0.2025730363060985,
#     284: -0.21096436465367321,
#     285: -0.21463736623412238,
#     286: -0.21445451406838514,
#     287: -0.21195316280969476,
#     288: -0.20867079680384037,
#     289: -0.20614490039661096,
#     290: -0.20591295793379552,
#     291: -0.2095124537611831,
#     292: -0.21840552551900932,
#     293: -0.2322353888333372,
#     294: -0.2487772671200113,
#     295: -0.26572122086994526,
#     296: -0.2807573105740524,
#     297: -0.29157559672324573,
#     298: -0.29586613980843857,
#     299: -0.29131906532130153,
#     300: -0.27670204068563187,
#     301: -0.254597010274873,
#     302: -0.22842949016802364,
#     303: -0.20162499644408563,
#     304: -0.1776090451820577,
#     305: -0.15980715246093952,
#     306: -0.1516448343597308,
#     307: -0.15622142724315674,
#     308: -0.17159430398573786,
#     309: -0.1918553111632621,
#     310: -0.2109893029486228,
#     311: -0.22298113351471344,
#     312: -0.22181565703442696,
#     313: -0.2014777276806567,
#     314: -0.15595901802969073,
#     315: -0.08271494958223735,
#     316: 0.011710124436089335,
#     317: 0.11929907566645778,
#     318: 0.23203477575003606,
#     319: 0.3419000963279925,
#     320: 0.4408779090414953,
#     321: 0.5209510855317125,
#     322: 0.5746978925402423,
#     323: 0.6009767093208757,
#     324: 0.6024124889965041,
#     325: 0.5816813107449703,
#     326: 0.5414592537441173,
#     327: 0.48442239717178776,
#     328: 0.41324682020582526,
#     329: 0.33061440552730026,
#     330: 0.23994613949315255,
#     331: 0.14611873395406733,
#     332: 0.054179019307607656,
#     333: -0.03082617404865161,
#     334: -0.1038500157171474,
#     335: -0.1598456753003125,
#     336: -0.19376632240058006,
#     337: -0.20151635492904912,
#     338: -0.18612321416903124,
#     339: -0.15384361710927477,
#     340: -0.11095031655013475,
#     341: -0.06371606529196613,
#     342: -0.01841361613512213,
#     343: 0.018684278120038495,
#     344: 0.041394150308679455,
#     345: 0.048400154222470446,
#     346: 0.04566248692728556,
#     347: 0.03973684960147778,
#     348: 0.03717894342340014,
#     349: 0.04454446957140562,
#     350: 0.06838912922384718,
#     351: 0.11526862355907791,
#     352: 0.18905823362230725,
#     353: 0.2790448311031071,
#     354: 0.36957622089706815,
#     355: 0.44499627461985897,
#     356: 0.48964886388714807,
#     357: 0.4878778603146048,
#     358: 0.4240271355178969,
#     359: 0.28345370443943657,
#     360: 0.08151677404315823,
#     361: -0.1321477014681692,
#     362: -0.3060309560867236,
#     363: -0.3886242238046612,
#     364: -0.32841873861416193,
#     365: -0.07390573450739724,
#     366: 0.4264235545234605
# }

# COMMAND ----------

# import datetime
# import warnings

# import numpy as np
# import pandas as pd
# import pyspark.sql.functions as pyf
# import pyspark.sql.types as pyt
# from pyspark.sql import DataFrame
# from pyspark.sql.window import Window

# from acosta.alerting.helpers import check_path_exists

# from acosta.alerting.helpers import features as acosta_features
# #from pyspark.sql.session import SparkSession
# # spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# def _list_days(start_date, end_date):
#     """
#       Take an RDD Row with startDate and endDate and produce a list of
#       all days including and between start and endDate.

#       :return: list of all possible days including and between start and end date.
#     """
#     day_range = (end_date - start_date).days + 1
#     return [start_date + datetime.timedelta(i) for i in range(day_range)]

# COMMAND ----------

# def _all_possible_days(df, date_field, join_fields_list):
#     """
#     Return the data frame with all dates represented from
#     the min and max of the <date_field>.

#     :param DataFrame df:
#     :param string date_field: The column name in <df>
#     containing a date type.
#     :param list(string) join_fields_list: A list of columns
#     that uniquely represent a row.

#     :return DataFrame:
#     """
#     min_max_date = df.groupBy('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID').agg(
#         pyf.min(date_field).alias('startDate'),
#         pyf.max(date_field).alias('endDate')
#     )

#     # This should be moved into a Dataframe rather than RDD to increase the chance of pipelining
#     explode_dates = min_max_date.rdd \
#         .map(lambda r: (r.ORGANIZATION_UNIT_NUM, r.RETAILER_ITEM_ID, _list_days(r.startDate, r.endDate))) \
#         .toDF(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'dayList']) \
#         .select('ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', pyf.explode(pyf.col('dayList')).alias(date_field))

#     return explode_dates.join(df, [date_field] + join_fields_list, 'left')

# COMMAND ----------

# # Not necessary when using intermediate data that has any negatives and zeroes removed.
# def _replace_negative_and_null_with(df, cols, replacement_constant):
#     """
#     Given a DataFrame and a set of columns, replace any negative value
#       and null values with the replacement_constant.  Selects only
#       columns that are of a numeric Spark data type (DecimalType, DoubleType,
#       FloatType, IntegerType, LongType, ShortType).

#     :param DataFrame df: Contains the columns referenced by the cols list.
#     :param list cols: A list of columns that will be checked for zeros
#       and the nulls will be replaced with the replacement_constant.
#     :param int replacement_constant: Value that will be used instead of
#       the null values in the selected cols.

#     :return DataFrame df: The DataFrame with negative and null values
#       replaced with replacement_constant.
#     """
#     numeric_types = [pyt.DecimalType, pyt.DoubleType, pyt.FloatType, pyt.IntegerType, pyt.LongType, pyt.ShortType]

#     # TODO: Consider adding a check if replacement_constant is
#     # a numeric type, otherwise skip these loops

#     # Loop through columns, look for numeric types
#     numeric_cols = []
#     for struct in df.select(cols).schema:
#         for typ in numeric_types:
#             if isinstance(struct.dataType, typ):
#                 numeric_cols.append(struct.name)
#             else:
#                 continue

#     # Check if any of those columns contain negative values
#     # If so, replace with null
#     for num_col in numeric_cols:
#         df = df.withColumn(num_col, pyf.when(pyf.col(num_col) > 0, pyf.col(num_col)).otherwise(None))

#     # Use na.fill to replace with replacement_constant
#     df = df.na.fill(value=replacement_constant, subset=cols)

#     return df

# COMMAND ----------

# def cast_decimal_to_number(df: DataFrame, cast_to='double'):
#     """
#     Finds and casts decimal dtypes to another pandas_udf friendly number type

#     :param df:
#     :param cast_to:
#     :return:
#     """
#     for col_name, col_type in df.dtypes:
#         if 'decimal' in col_type:
#             df = df.withColumn(col_name, df[col_name].cast(cast_to))
#     return df

# def convert_dict_to_spark_df(d, col_names, sc):
#     a = [(key,) + (value,) for key, value in d.items()]
#     df = sc.parallelize(a).toDF(col_names)
#     return df

# def repeated_rbf(xin, center, period, std=1):
#     # DEPRECATED
#     n_repeats = int(xin.max() // period)
#     repeats = np.ones(n_repeats + 1) * np.arange(0, 1 + n_repeats) * 365

#     center_vec = repeats - (-center)
#     center_vec = np.tile(center_vec, (len(xin), 1))

#     diff = (xin - center_vec.T).T

#     mod = diff
#     return np.exp((-(mod) ** 2) / (2 * std ** 2)).max(axis=1)  # calling max on the row gets the strongest RBF

# COMMAND ----------

# def decompose_seasonality(df, year_level_resolution=52):
#     """
#     Returns 2 new DataFrames.
#     One contains the year seasonality and the other contains the week level seasonality.

#     DOY is the day of year
#     DOW is the day of week
#     PRICE is the price of the product. This is to de-confound the relationship between demand and price.

#     :param df: DataFrame must contain the [POS_ITEM_QTY, PRICE, DOY, DOW]
#     :param year_level_resolution: How much P-Splines to use to estimate the year seasonality
#     :return:
#     """
#     from pygam import LinearGAM, s, f

#     df.columns = [x.upper() for x in df.columns]

#     if 'POS_ITEM_QTY' not in df.columns or 'PRICE' not in df.columns or 'DOY' not in df.columns or 'DOW' not in df.columns:
#         raise ValueError('Input `df` MUST have columns [POS_ITEM_QTY, PRICE, DOY, DOW]')

#     decomposer = LinearGAM(s(0, n_splines=year_level_resolution, lam=0.25) + f(1, lam=0.25) + s(2, lam=0.25))
#     decomposer.fit(df[['DOY', 'DOW', 'PRICE']].values, df['POS_ITEM_QTY'].values)

#     # Decompose Seasonality Trends
#     n_non_intercept_terms = len(decomposer.terms) - 1

#     doy_range = np.arange(df['DOY'].min(), df['DOY'].max()+1)
#     x_grid_year = np.zeros((len(doy_range), n_non_intercept_terms))
#     x_grid_year[:, 0] = doy_range

#     dow_range = np.arange(df['DOW'].min(), df['DOW'].max()+1)
#     x_grid_week = np.zeros((len(dow_range), n_non_intercept_terms))
#     x_grid_week[:, 1] = dow_range

#     # Compute partial dependence
#     year_partial_dependence, _ = decomposer.partial_dependence(0, x_grid_year, width=0.95)
#     week_partial_dependence, _ = decomposer.partial_dependence(1, x_grid_week, width=0.95)

#     # Create the 2 output DataFrames
#     df_year_seasonality = pd.DataFrame({'DOY': doy_range, 'YEAR_SEASONALITY': year_partial_dependence})
#     df_week_seasonality = pd.DataFrame({'DOW': dow_range, 'WEEK_SEASONALITY': week_partial_dependence})
#     return df_year_seasonality, df_week_seasonality

# COMMAND ----------

# def compute_reg_price_and_discount_features(df, threshold=0.025, max_discount_run=49):
#     """
#     Computes the REG_PRICE, DISCOUNT_PERCENT & DAYS_DISCOUNTED from a dataframe that contains
#     a PRICE, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID

#     :param df:
#     :param threshold: arbitrary threshold required to call price changes significant
#     :param max_discount_run: How many days a discount needs to run before it is considered the REG_PRICE should be
#         changed to match the PRICE
#     :return:
#     """
#     # Create pricing dummy columns

#     df_pricing_logic = df[['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'SALES_DT', 'PRICE']]
#     df_pricing_logic = df_pricing_logic \
#         .withColumn('REG_PRICE', pyf.lit(0.0)) \
#         .withColumn('DISCOUNT_PERCENT', pyf.lit(0.0)) \
#         .withColumn('DAYS_DISCOUNTED', pyf.lit(0.0))

#     @pyf.pandas_udf(df_pricing_logic.schema, pyf.PandasUDFType.GROUPED_MAP)
#     def pricing_data_udf(d: pd.DataFrame) -> pd.DataFrame:
#         """
#         This generates the REG_PRICE, DISCOUNT_PERCENT, DAYS_DISCOUNTED from the PRICE
#         using fancy pants logic

#         NOTE: Pandas UDF cannot have any NANs in it at all and more importantly cannot produce a NAN anywhere.
#             All the code below was used to make sure that NANs never occur.

#         :param d:
#         :return:
#         """
#         # NOTE: Function is nested to use the extra args `threshold` and `max_discount_run`
#         d = d.sort_values('SALES_DT')
#         d.loc[:, 'PRICE'] = d['PRICE']\
#             .interpolate(method='nearest', limit_direction='both')\
#             .fillna(method='ffill')\
#             .fillna(method='bfill')

#         # Check if some columns still contain NaNs
#         null_check_data = d.isnull().sum()
#         if null_check_data.any():
#             # Some columns still contain NaNs this should only be possible if ALL data in the calculated price is nan
#             if null_check_data['PRICE'] == len(d):
#                 fill_value = -1.0
#                 warnings.warn(
#                     'All data in the `PRICE` price column in NaNs. Setting all to {}'.format(fill_value)
#                 )
#                 d.loc[:, 'PRICE'] = fill_value
#                 return d
#             else:
#                 raise AssertionError('Pandas UDF contains unexpected {} NaNs. N = {} \n NaNs found at \n{}'.format(
#                     null_check_data,
#                     len(d),
#                     d[null_check_data.any(axis=1)]
#                 ))

#         # Row wise business logic
#         for i, _ in d.iterrows():
#             if i == 0:
#                 d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                 continue

#             calc_price_delta = d.loc[i, 'PRICE'] - d.loc[i - 1, 'PRICE']
#             ratio = (d.loc[i, 'PRICE'] - d.loc[i - 1, 'REG_PRICE']) / d.loc[i - 1, 'REG_PRICE']
#             if calc_price_delta > 0:
#                 # Calculated price increase
#                 if ratio >= threshold:
#                     # Significant price increase
#                     # Check for index out of bounds error
#                     if i < 2:
#                         # Computing yesterday_ratio will trigger an KeyError for the index being out of bounds
#                         d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                     else:
#                         # Check if this has continued for 2 days
#                         yesterday_ratio = (d.loc[i - 1, 'PRICE'] - d.loc[i - 2, 'REG_PRICE'])
#                         yesterday_ratio = yesterday_ratio / d.loc[i - 2, 'REG_PRICE']
#                         if yesterday_ratio >= threshold:
#                             # Set as a real price increase and back fill *(Still don't like this idea)
#                             d.loc[i - 1, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                             d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                         else:
#                             # Carry over yesterday's data for now
#                             d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                 else:
#                     # Insignificant price increase
#                     d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                     # Check if the increase is still in a discount
#                     if ratio <= -threshold:
#                         d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
#                         d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
#                         d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']

#             elif calc_price_delta < 0:
#                 # Calculated price decrease
#                 if ratio <= -threshold:
#                     # This is a discount therefore start counting
#                     d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                     d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
#                     d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
#                     d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']
#                 else:
#                     # Insignificant price decrease
#                     d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

#             else:
#                 # Steady price
#                 d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

#                 # Increase steady-state logic
#                 if ratio >= threshold:
#                     if i < 2:
#                         # Computing yesterday_ratio will trigger an KeyError for the index being out of bounds
#                         d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                         continue

#                     # Check if this has continued for 2 days
#                     yesterday_ratio = (d.loc[i - 1, 'PRICE'] - d.loc[i - 2, 'REG_PRICE'])
#                     yesterday_ratio = yesterday_ratio / d.loc[i - 2, 'REG_PRICE']
#                     if yesterday_ratio >= threshold:
#                         # Set as a real price increase and back fill *(Still don't like this idea)
#                         d.loc[i - 1, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                         d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                     else:
#                         # Carry over yesterday's data for now
#                         d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']

#                 # Discount steady-state logic
#                 if ratio <= -threshold:
#                     # Discount
#                     if d.loc[i - 1, 'DAYS_DISCOUNTED'] < max_discount_run:
#                         # Discount is on going
#                         d.loc[i, 'REG_PRICE'] = d.loc[i - 1, 'REG_PRICE']
#                         d.loc[i, 'DAYS_DISCOUNTED'] = d.loc[i - 1, 'DAYS_DISCOUNTED'] + 1
#                         d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
#                         d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']
#                     else:
#                         # Discount is now the new price
#                         d.loc[i, 'REG_PRICE'] = d.loc[i, 'PRICE']
#                         d.loc[i, 'DAYS_DISCOUNTED'] = 0
#                         d.loc[i, 'DISCOUNT_PERCENT'] = (d.loc[i, 'REG_PRICE'] - d.loc[i, 'PRICE'])
#                         d.loc[i, 'DISCOUNT_PERCENT'] = d.loc[i, 'DISCOUNT_PERCENT'] / d.loc[i, 'REG_PRICE']

#         return d

#     df_pricing_logic = df_pricing_logic.groupBy(['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID']).apply(pricing_data_udf)
#     df_pricing_logic = df_pricing_logic.drop('PRICE')  # dropping because `df` contains the calculated price

#     df = df.join(df_pricing_logic, ['ORGANIZATION_UNIT_NUM', 'RETAILER_ITEM_ID', 'SALES_DT'], 'inner')
#     return df

# COMMAND ----------

# # TODO: Make this more generic (lags and leads)
# def compute_price_and_lag_lead_price(df, total_lags=14, threshold=0.025, max_discount_run=49):
#     """
#     Calculate the price and populate any null price as
#     the previous non-null price within the past 14 days.
#     Price is calculated as POS_AMT divided by POS_ITEM_QTY.
#     The column PRICE is added to the dataframe.

#     :param total_lags:
#     :param threshold: arbitrary threshold required to call price changes
#      significant
#     :param max_discount_run: How many days a discount needs to run before it is
#      considered the REG_PRICE should be
#         changed to match the PRICE
#     :param DataFrame df: Includes columns for POS_ITEM_QTY,
#     POS_AMT, ORGANIZATION_UNIT_NUM, RETAILER_ITEM_ID, and SALES_DT.
#     """

#     def create_lag_cols(input_df):
#         # Define how the partition of data the lags and leads should be calculated on
#         window_spec = Window.partitionBy(
#             ['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
#         ).orderBy('SALES_DT')

#         cols_to_drop, coalesce_funcs = [], []
#         for nth_lag in range(1, total_lags + 1):
#             col_name = 'LAG{}'.format(nth_lag)
#             input_df = input_df.withColumn(
#                 col_name, pyf.lag(pyf.col('PRICE'), nth_lag
#                                   ).over(window_spec))

#             coalesce_funcs.append(
#                 pyf.when(
#                     pyf.isnull(pyf.col(col_name)), None
#                 ).when(
#                     pyf.col(col_name) == 0, None
#                 ).otherwise(
#                     pyf.col(col_name)
#                 )
#             )
#             cols_to_drop.append(col_name)

#         return input_df, cols_to_drop, coalesce_funcs

#     # Calculate pricing case / when style
#     df_priced = df.withColumn(
#         'PRICE',
#         pyf.when(
#             (pyf.col('POS_ITEM_QTY') == 0) |
#             (pyf.col('POS_AMT') == 0) |
#             (pyf.isnan(pyf.col('POS_ITEM_QTY'))) |
#             (pyf.isnan(pyf.col('POS_AMT')))
#             , None
#         ).otherwise(pyf.col('POS_AMT') / pyf.col('POS_ITEM_QTY'))
#     )
#     df = None

#     # Apply the window to 14 lags
#     # At the end, the calculated Price follows these rules -
#     # Take the original price if it is not null
#     # Take the next most recent price, with lag (i.e. the day in the past)
#     # first.
#     df_lagged, columns_to_drop, coalesce_functions = create_lag_cols(df_priced)
#     df_priced = None

#     df_calculated = df_lagged.withColumn(
#         'CALCULATED_PRICE',
#         pyf.coalesce(
#             pyf.when(pyf.isnull(pyf.col('PRICE')), None)\
#             .when(pyf.col('PRICE') == 0, None)\
#             .otherwise(pyf.col('PRICE')),
#             *coalesce_functions
#         )
#     )
#     df_lagged = None
#     # Price has nulls. We delete Price in rename Calculated_Price to Price.
#     columns_to_drop += ['PRICE']
#     df_calculated = df_calculated.drop(*columns_to_drop)
#     df_calculated = df_calculated.withColumnRenamed('CALCULATED_PRICE', 'PRICE')
#     return df_calculated

# COMMAND ----------

# def pos_to_training_data(
#         df, retailer, client, country_code, model_source, spark, spark_context, include_discount_features=False,
#         item_store_cols=None
# ):
#     """
#     Takes in a POS DataFrame and:
#     - Explodes data to include all days between min and max of original dataframe.
#     - Computes Price and a lag/leading price.
#     - Joins global tables:
#       - holidays_usa
#     - Relies on a global DATE_FIELD being defined.
  
#     :param DataFrame df: A POS fact table containing <item_store_cols>, POS_ITEM_QTY, POS_AMTS
#       and the field defined in a constant <DATE_FIELD>.
#     :param string retailer: A string containing a valid RETAILER.
#     :param string client: A string containing a valid CLIENT for the given RETAILER.
#     :param string country_code: A string containing a valid COUNTRY CODE for the given RETAILER/CLIENT.
#     :param string model_source: Whether the location of the model source data is local or production.    
#     :param SparkSession spark: The current spark session.
#     :param SparkContext spark_context: In databricks you get this for free in the `sc` object
#     :param boolean include_discount_features: Whether or not the advanced discount features should be
#         calculated.  Setting to true provides
#     :param list(string) item_store_cols: The list of column names that represent the RETAILER_ITEM_ID
#       and stores and are used in joining to NARS datasets (Note: deprecated).
#     :param is_inference: if this is True then it MUST look up a previously computed seasonality
#         for the store and items it is looking for.
  
#     :return DataFrame:
#     """
#     from pygam.utils import b_spline_basis

#     # HIGH LEVEL COMMENT: The idea of setting the variables to None was done with the hope
#     #   of keeping the driver memory low.  However, since that is no longer necessary,
#     #   consider overwriting the variables for cleanliness or stick with the unique
#     #   variables to keep it scala-esque.

#     # Necessary to handle the Pricing logic
#     # @Stephen Rose for more details
#     df = cast_decimal_to_number(df, cast_to='float')

#     df010 = _all_possible_days(df=df, date_field='SALES_DT', join_fields_list=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'])

#     # Fill in the zeros for On hand inventory quantity
#     # Could be merged with the _replace_negative_and_null_with
#     # Should verify that the optimizer is not combining this already
#     df110 = df010.withColumn(
#         'ON_HAND_INVENTORY_QTY',
#         pyf.when(pyf.col('ON_HAND_INVENTORY_QTY') < 0, pyf.lit(0)).otherwise(
#             pyf.col('ON_HAND_INVENTORY_QTY'))
#         )

#     df010 = None
#     df020 = _replace_negative_and_null_with(df=df110, cols=['POS_ITEM_QTY', 'POS_AMT'], replacement_constant=0)

#     # Very important to get all of the window functions using this partitioning to be done at the same time
#     # This will minimize the amount of shuffling being done and allow for pipelining
#     window_item_store = Window.partitionBy(['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']).orderBy('SALES_DT')

#     # Lag for 12 weeks to match the logic from the LOESS baseline model
#     # TODO: Increase lag to a year once we have more data
#     # TODO: replace with for loop of the acosta_features.get_lag_days()
#     lag_units = list(range(1, 31)) + [35, 42, 49, 56, 63, 70, 77, 84]
#     for lag in lag_units:

#         # 2019-03-19: We are not currently using lag units, so this code may be removed in the future
#         df020 = df020.withColumn(
#             'LAG_UNITS_{}'.format(lag),
#             pyf.lag(pyf.col('POS_ITEM_QTY'), lag).over(window_item_store)
#         )
#         if lag <= 14:

#             # Lag inventory units are created to generate the recent_on_hand_inventory_diff feature
#             df020 = df020.withColumn(
#                 'LAG_INV_{}'.format(lag),
#                 pyf.lag(pyf.col('ON_HAND_INVENTORY_QTY'), lag).over(window_item_store)
#             )

#     # Include the lags for pricing
#     # This note is still very much important to avoid a second shuffle
#     # NOTE: Consider moving this out of a function and embedding into pos_to_training_data
#     # NOTE: (continued) the DAG shows two window functions being used instead of one.
#     df020 = compute_price_and_lag_lead_price(df020)

#     # Use the Lag_INV columns to generate RECENT_ON_HAND_INVENTORY_QTY
#     # The Lag_INV columns are dropped after this command and are no longer referenced
#     df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_QTY', pyf.coalesce(
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_1')), None).otherwise(pyf.col('LAG_INV_1')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_2')), None).otherwise(pyf.col('LAG_INV_2')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_3')), None).otherwise(pyf.col('LAG_INV_3')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_4')), None).otherwise(pyf.col('LAG_INV_4')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_5')), None).otherwise(pyf.col('LAG_INV_5')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_6')), None).otherwise(pyf.col('LAG_INV_6')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_7')), None).otherwise(pyf.col('LAG_INV_7')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_8')), None).otherwise(pyf.col('LAG_INV_8')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_9')), None).otherwise(pyf.col('LAG_INV_9')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_10')), None).otherwise(pyf.col('LAG_INV_10')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_11')), None).otherwise(pyf.col('LAG_INV_11')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_12')), None).otherwise(pyf.col('LAG_INV_12')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_13')), None).otherwise(pyf.col('LAG_INV_13')),
#                 pyf.when(pyf.isnan(pyf.col('LAG_INV_14')), None).otherwise(pyf.col('LAG_INV_14'))
#             )).drop(*['LAG_INV_{}'.format(i) for i in range(1, 15)])
    
#     # The RECENT_ON_HAND_INVENTORY_DIFF is the prior day end's inventory minus two days ago
#     # RECENT_ON_HAND_INVENTORY_QTY is at least the prior day's ending inventory
#     df020 = df020.withColumn('RECENT_ON_HAND_INVENTORY_DIFF', pyf.col('RECENT_ON_HAND_INVENTORY_QTY') \
#                             - pyf.lag(pyf.col('RECENT_ON_HAND_INVENTORY_QTY'), 1).over(window_item_store))

#     df110 = None
#     loaded_df01 = None
#     # TODO (5/22/2019) check if the table exists and is not empty.
#     # Reference data that contains Holidays, Days of Week, and Days of Month
    
#     country_code = country_code.upper()

#     sql_statement = 'SELECT * FROM retail_forecast_engine.country_dates WHERE COUNTRY = "{country}"'
#     sql_statement = sql_statement.format(country=country_code)
    
#     master_dates_table = spark.sql(sql_statement)

#     # NOTE!  The Country/Date dimension contains some columns we're not yet ready to use in our training process.
#     # This includes the new "RELATIVE" class of columns
#     # So let's filter out those columns
#     master_dates_table = master_dates_table.drop(*["COUNTRY", "DAY_OF_WEEK"])
#     master_dates_table = master_dates_table.drop(*filter(lambda x: x.endswith("_RELATIVE"), master_dates_table.schema.names))

#     # This is all @Stephen
#     # TODO (5/13/2019) [FUTURE] swap with item-client specific seasonality
#     # Add Seasonality Columns
#     master_dates_table = master_dates_table.withColumn('dow', pyf.dayofweek('SALES_DT'))
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))

#     week_df = convert_dict_to_spark_df(week_seasonality_spark_map, ['dow', 'WEEK_SEASONALITY'], spark_context)
#     year_df = convert_dict_to_spark_df(year_seasonality_map, ['doy', 'YEAR_SEASONALITY'], spark_context)

#     master_dates_table = master_dates_table.join(week_df, week_df['dow'] == master_dates_table['dow'])
#     master_dates_table = master_dates_table.drop('dow')

#     master_dates_table = master_dates_table.join(year_df, year_df['doy'] == master_dates_table['doy'])
#     master_dates_table = master_dates_table.drop('doy')

#     # Add dynamic intercepts (time of year effect)
#     days_of_year = np.arange(366) + 1
#     dynamic_intercepts = b_spline_basis(
#         days_of_year,
#         edge_knots=np.r_[days_of_year.min(), days_of_year.max()],
#         sparse=False, periodic=False
#     )
#     dynamic_intercepts = pd.DataFrame(
#         dynamic_intercepts,
#         columns=['TIME_OF_YEAR_{}'.format(i) for i in range(dynamic_intercepts.shape[1])]
#     )
#     dynamic_intercepts['doy'] = days_of_year

#     dynamic_intercepts = spark.createDataFrame(dynamic_intercepts)
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
#     master_dates_table = master_dates_table.join(
#         dynamic_intercepts, dynamic_intercepts['doy'] == master_dates_table['doy']
#     )
#     master_dates_table = master_dates_table.drop('doy')

#     # Add weekly amplitude features
#     master_dates_table = master_dates_table.withColumn('doy', pyf.dayofyear('SALES_DT'))
#     result_df = master_dates_table.toPandas()
#     amplitude = ['WEEK_AMPLITUDE_{}'.format(i) for i in range(15)]
#     for i, week_i in enumerate(amplitude):
#         result_df[week_i] = repeated_rbf(
#             result_df['doy'].values,
#             i * (366 / len(amplitude)),
#             period=366,
#             std=10
#         )
#         result_df[week_i] = result_df[week_i] * result_df['WEEK_SEASONALITY']
#     master_dates_table = spark.createDataFrame(result_df)

#     # Load reference data for Snap and Paycycles
#     # Joins to the data set based on day of month and organization_unit_num (store #)
#     snap_pay_cycle = spark.read.parquet(
#         '/mnt{mod}/artifacts/country_code/reference/snap_paycycle/retailer={retailer}/client={client}/country_code={country_code}/'.format(
#             mod='' if model_source == 'LOCAL' else '/prod-ro',
#             retailer=retailer.lower(),
#             client=client.lower(),
#             country_code=country_code.lower()
#         )
#     ).withColumnRenamed("DAYOFMONTH", "DAY_OF_MONTH")

#     print('0)', snap_pay_cycle.count())
    
#     # Join to the list of holidays, day_of_week, and day_of_month
#     # This join is needed before joining snap_paycycle due to the presence of day_of_month
#     df_with_dates = df020.join(master_dates_table, ["SALES_DT"], "inner")

#     print('1)', df_with_dates.count())
      
#     df_with_snap = df_with_dates \
#         .join(snap_pay_cycle, ['ORGANIZATION_UNIT_NUM', 'DAY_OF_MONTH'], 'inner') \
#         .drop('STATE')

#     print('2)', df_with_snap.count())
      
#     df_penultimate = df_with_snap
#     df_with_dates = None
#     master_dates_table = None
#     snap_pay_cycle = None
#     loaded_df02 = None

#     if include_discount_features:
#         df_out = compute_reg_price_and_discount_features(df_penultimate, threshold=0.025, max_discount_run=49)
#     else:
#         df_out = df_penultimate

#     df_penultimate = None

#     # Add day of the week
#     df_out = df_out.withColumn('DOW', pyf.dayofweek('SALES_DT'))
    
#     return df_out

# COMMAND ----------

dbutils.widgets.text('retailer', 'walmart', 'Retailer')
dbutils.widgets.text('client', 'clorox', 'Client')
dbutils.widgets.text('countrycode', 'us', 'Country Code')

dbutils.widgets.text('store', '', 'Organization Unit Num')
dbutils.widgets.text('item', '', 'Retailer Item ID')
dbutils.widgets.text('runid', '', 'Run ID')

dbutils.widgets.dropdown('MODEL_SOURCE', 'local', ['local', 'prod'], 'Model Source')
dbutils.widgets.dropdown('INCLUDE_DISCOUNT_FEATURES', 'No', ['Yes', 'No'], 'Include Discount Features')

RETAILER = dbutils.widgets.get('retailer').strip().lower()
CLIENT = dbutils.widgets.get('client').strip().lower()
COUNTRY_CODE = dbutils.widgets.get('countrycode').strip().lower()

RUN_ID = dbutils.widgets.get('runid').strip()
MODEL_SOURCE = dbutils.widgets.get('MODEL_SOURCE').upper()
MODEL_SOURCE = 'LOCAL' if MODEL_SOURCE.startswith('LOCAL') else 'PROD'
INCLUDE_DISCOUNT_FEATURES = dbutils.widgets.get('INCLUDE_DISCOUNT_FEATURES').strip().lower() == 'yes'

try:
    STORE = int(dbutils.widgets.get('store').strip())
except ValueError:
    STORE = None
try:
    ITEM = str(dbutils.widgets.get('item').strip())
except ValueError:
    ITEM = None

if RETAILER == '':
    raise ValueError('\'retailer\' is a required parameter.  Please provide a value.')

if CLIENT == '':
    raise ValueError('\'client\' is a required parameter.  Please provide a value.')

if COUNTRY_CODE == '':
    raise ValueError('\'countrycode\' is a required parameter. Please provide a value.')

if RUN_ID == '':
    RUN_ID = str(uuid.uuid4())

# PATHS
PATH_DATA_VAULT_TRANSFORM_OUTPUT = '/mnt/processed/training/{run_id}/data_vault/'.format(run_id=RUN_ID)
PATH_ENGINEERED_FEATURES_OUTPUT = '/mnt/processed/training/{run_id}/engineered/'.format(run_id=RUN_ID)
PATH_TEMP_DIR_OUTPUT = '/mnt/processed/training/{run_id}/temp/'.format(run_id=RUN_ID)

for param in [RETAILER, CLIENT, COUNTRY_CODE, STORE, ITEM, RUN_ID]:
    print(param)

if CLIENT.lower() not in RUN_ID.lower():
    warnings.warn('Potentially uninformative RUN_ID, {} is not in the RUN_ID'.format(CLIENT))

# COMMAND ----------

data = spark.read.format('delta').load(PATH_DATA_VAULT_TRANSFORM_OUTPUT)
print(data.dtypes)

# COMMAND ----------

display(data)

# COMMAND ----------

if STORE:
    data = data.filter('ORGANIZATION_UNIT_NUM == "{}"'.format(STORE))

if ITEM:
    data = data.filter('RETAILER_ITEM_ID == "{}"'.format(ITEM))

# This filter requires at least 84 days of non-zero sales in the entire dataset
subset_meets_threshold = data\
    .select('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM', 'POS_ITEM_QTY') \
    .filter('POS_ITEM_QTY > 0') \
    .groupBy('RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM') \
    .count() \
    .filter('count >= 84') \
    .drop('count')

data = data.join(
    subset_meets_threshold,
    ['RETAILER', 'CLIENT', 'COUNTRY_CODE', 'RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM'],
    'inner'
)

# COMMAND ----------

output_data = pos_to_training_data(
    df=data,
    retailer=RETAILER,
    client=CLIENT,
    country_code=COUNTRY_CODE,
    model_source=MODEL_SOURCE,
    spark=spark,
    spark_context=sc,
    include_discount_features=INCLUDE_DISCOUNT_FEATURES,
    item_store_cols=['RETAILER_ITEM_ID', 'ORGANIZATION_UNIT_NUM']
)

data = None

# COMMAND ----------

_ = [print(name, dtype) for name, dtype in output_data.dtypes]
