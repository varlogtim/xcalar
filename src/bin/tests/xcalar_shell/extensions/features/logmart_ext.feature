Feature: logmart
  As a user,
  I want to use logmart command in xcalar shell,
  so that I can upload log files and populate tables to Xcalar Notebook

  Background:
    Given I am on xcalar shell terminal

  Scenario: Refresh LogMart tables from cluster logs
    When I enter "logmart -refresh <log_root> " in the command line
    Then I should see "xxx" output


  Scenario: Refresh LogMart tables from specified ASUP location
    When I input "logmart -load_asup <asup_path> target <target_name>" in the command line
    Then I can see "xxx" output


  Scenario: Load LogMart tables from the specified untarred loacation
    When I type "logmart -load_logs <logs_path> target <target_name>" in the command line
    Then I do see "xxx" output