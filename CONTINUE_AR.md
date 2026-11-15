# متابعة مشروعكم الأصلي

هذه النسخة مبنية على projectTeam-code.zip الذي أرسلته. REPORT.md محفوظ دون تعديل.
ملفات الفحص القديمة موجودة بأسمائها؛ أضفنا الاستكمال إلى jobs وstreaming.
لا تستبدل بياناتك أو تحذف المجلد القديم. فك الضغط في مجلد منفصل أولاً ثم راجع الملفات المعدّلة.

## الملفات والبيانات

الكود وحده مرفق. بيانات IMDb الحالية على USB لا تزال عندك.
انسخ .env.example إلى .env، ثم اضبط مثلاً:

```dotenv
IMDB_DATA_DIR=D:/projectTeam/data
IMDB_RESULTS_DIR=D:/projectTeam/results
IMDB_TEMP_DIR=D:/projectTeam/spark-tmp
INPUT_FORMAT=tsv
```

يمكنك اختيار D:/imdb-fresh بدلاً من D:/projectTeam/data إذا كانت الملفات السليمة هناك داخل raw.
كل الجداول السبعة يجب أن تكون في raw تحت مجلد البيانات المختار.
هذه الإعدادات تنقل بيانات المشروع ونتائجه ومؤقتات Spark؛ تخزين صور Docker نفسه يبقى في مكانه الحالي.

## الخطوة الأولى: التأكد من الملفات الموجودة

من PowerShell داخل مجلد الكود:

```powershell
py -3.13 scripts/download_imdb.py --data D:/projectTeam/data --verify-only
```

إذا كانت ملفات تالفة أو ناقصة، الأمر التالي ينزّلها على USB مباشرة، ويتحقق من الضغط قبل استبدال الملف التالف؛ الملفات السليمة لا تُحمّل مجدداً:

```powershell
py -3.13 scripts/download_imdb.py --data D:/projectTeam/data --repair
```

لا حاجة لإعادة بناء كل Parquet أولاً؛ قارئ البيانات يستخدم TSV افتراضياً لتفادي النسخ السابقة التالفة.

## الخطوة الثانية: تشغيل حاويتك والتأكد من ربط USB

```powershell
docker compose up -d --no-deps pyspark-notebook
docker compose exec pyspark-notebook ls -l /home/jovyan/scripts/clean_values.py /home/jovyan/data/raw/title.basics.tsv.gz
```

إذا كانت الملفات غير مرئية رغم وجودها في Windows، توقف هنا: ربط USB ما زال غير سليم.
لا تشغّل المهام على مجلد فارغ. نستخدم docker compose exec كما في تشغيلك الناجح، وليس run الذي توقف عند sudo: spark-submit: command not found.

## استئناف التنظيف والتحليل

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Clean
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Step Analyze -Question q1
```

اختر q2 إلى q8 أو open أو all. التنظيف القديم يكتب data/parquet/staged وdata/quality كما سابقاً.
الاستئناف الآن يتحقق من بصمة المصدر والناتج؛ أول تشغيل بهذه النسخة يعيد التحقق/الحساب لأن العلامات القديمة ليست إثبات سلامة.
للعمل على جدول واحد:

```powershell
docker compose exec -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark-notebook spark-submit --driver-memory 4g --conf spark.local.dir=/spark-tmp scripts/clean_values.py --tables title.episode
```

التحليل يطبق نفس قواعد القيم، ثم يستبعد المفاتيح المفقودة/المكررة والمراجع اليتيمة في views؛ لا يحتاج نسخة كاملة إضافية.

## بقية الأجزاء

أوامر جمع ويكيبيديا وStreaming موجودة في RUN_GUIDE.md.
نتائج التحليل الجديدة تُكتب في results؛ ليست مرفقة كأنها نُفذت على IMDb.
نتائج الاختبار الصغيرة موضّحة في VALIDATION.md، ولا يجوز تقديمها كنتائج البيانات الحقيقية.
التقرير تركناه لكم كما طلبت. ارفع الكود في فرع وPull Request وفق تنظيم المجموعة، دون بيانات IMDb أو .env.
