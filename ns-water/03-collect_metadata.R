
library(arrow)

feature_codes <- readxl::read_excel("ns-water/nshn_v2/NSHN_FEATURECODES.xls")
arrow::write_parquet(
  feature_codes,
  "ns-water/ns-water_feature-codes.parquet",
  compression = "uncompressed"
)

file.copy(
  "ns-water/nshn_v2/NSHN Attribute_Specs.pdf",
  "ns-water/ns-water_attribute-specs.pdf"
)
