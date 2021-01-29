Feature: systemstat
  As a user,
  I want to use systemstat command in xcalar shell,
  so that I can monitor system stats


  Background:
    Given I am on xcalar shell terminal

  Scenario: check cpu stats
    When I enter "systemstat -n 0 -mpstat" in the command line
    Then I would see "cpu stats" output

  Scenario: check memory stats
    When I enter "systemstat -n 0 -vmstat" in the command line
    Then I should see "memory stats" output


  Scenario: system-cgls
    When I enter "systemstat -n 0 -systemd-cgls" in the command line
    Then I could see "systemd-cgls" output


  Scenario: cgroup Controller until
    When I enter "systemstat -n 0 -cgroupControllerUtil <options>" in the command line
    Then I do see "cgroupControllerUtil" output
