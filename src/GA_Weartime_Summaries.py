import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.pyplot as plt

# plot summary data weartime
weardata = '/Users/psaltd/Desktop/achondroplasia/ACH_GA_Weartime_Summaries.xlsx'
df = pd.read_excel(weardata)
df['site'] = [x.split('-')[0] for x in df.SUB_ID]
df = df[(df.site == 'GBR') | (df.site == 'DNK')]

sns.barplot(data=df, x='SUB_ID', y='avg_weartime', hue='location')
plt.xticks(rotation=45)
plt.tight_layout()
plt.xlabel('subject')
plt.ylabel('avg daily weartime (hrs)')
plt.title('C4181001 - GENEActiv Wear Compliance (Visit 1)')
# plt.savefig('/Users/psaltd/Desktop/achondroplasia/ACH_GA_DailyWeartime_Summaries.png')
plt.savefig('/results/ACH_GA_DailyWeartime_Summaries.png')

sns.barplot(data=df, x='SUB_ID', y='Monitoring_days', hue='location')
plt.xticks(rotation=45)
plt.tight_layout()
plt.xlabel('subject')
plt.ylabel('Number of Monitoring Days')
plt.title('C4181001 - GENEActiv Monitoring Duration (Visit 1)')
# plt.savefig('/Users/psaltd/Desktop/achondroplasia/ACH_GA_MonitoringDuration_Summaries.png')
plt.savefig('./results/ACH_GA_MonitoringDuration_Summaries.png')
