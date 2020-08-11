version 1.0

workflow CombineParticipantSets {

input {
  File cohort_set_tsv
  File case_set_tsv
}

}

task CombineAttributesForJointCalling {
  input {
    File cohort_set_tsv
    File case_set_tsv
    Array[String] attributes_from_cohort
    Array[String] attributes_to_concatenate
  }

  command <<<
  >>>

  output {

  }
}