from pyspark.sql.types import *


works_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("authorships", ArrayType(StructType([
        StructField("author", StructType([
            StructField("id", StringType()),
            StructField("display_name", StringType())
        ])),
        StructField("institutions", ArrayType(StructType([
            StructField("id", StringType()),
            StructField("display_name", StringType()),
            StructField("country_code", StringType())
        ]))),
        StructField("author_position", StringType())
    ]))),
    StructField("host_venue", StructType([
        StructField("id", StringType()),
        StructField("display_name", StringType()),
        StructField("publisher", StringType())
    ])),
    StructField("cited_by_count", IntegerType()),
    StructField("publication_year", IntegerType()),
    StructField("open_access", StructType([
        StructField("is_oa", BooleanType())
    ]))
])