# StorageClass-AWS
在Amazon S3存储桶中添加⽂件，会触发Amazon lambda，对⽂件进⾏更进⼀步的分析与处理，在⽂件被删除时，也会触发Amazon lambda，将⽣成的上述记录删除。主要实现了以下三点功能：
1. 图⽚分类。使⽤Amazon rekognition服务，对上传的图⽚进⾏分类，将分类结果存⾄Amazon DynamoDB数据库中。DynamoDB中，以<tag, uuid, bucket, name>四元组存储每个⽂件的每个类别，四元组中的四项分别表
⽰类别、⽂件独⼀⽆⼆的编号、⽂件所在桶、⽂件在桶内的位置。其中，tag、uuid为查询的关键字。为⽅便删除，在另⼀个DynamoDB数据库中，为每个⽂件存储⼀个<path, alltag>的⼆元组，记录⽂件路径到分类结果的
映射。
2. ⽂本朗读。从S3存储桶中读取上传的⽂件，使⽤Amazon polly服务，将上传的⽂本⽂件转成mp3格式的语⾳，存⾄另⼀个Amazon S3存储桶⾥。
3. 机器翻译。从S3存储桶中读取上传的⽂件，使⽤Amazon translate服务，将上传的英⽂⽂本翻译成中⽂，存⾄另⼀个Amazon S3存储桶内。
