version 1.1
# from https://github.com/chanzuckerberg/miniwdl/issues/635

struct Name {
  String first
  String last
}

struct Income {
  Float amount
  String period
  String? currency
}

struct Person {
  Name name
  Int age
  Income? income
  Map[String, File] assay_data
}
