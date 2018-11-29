Feature: Load My Data From a CSV File

  Scenario: File load landing
  	Given Given a file "test.txt" is present in staging area
    When  ETL process has run and "sample_log.txt" generated
    Then  Count should match between the file and table
    And Record "1" should exists in landing table
