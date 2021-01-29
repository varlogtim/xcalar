Feature: monitor
  As a user,
  I want to use "monitor" command in xcalar shell,
  so that I can monitor systems stats

  Background:
    Given I am on xcalar shell terminal

  Scenario: monitor cpu stats
    When I enter "monitor -n 0 mpstat" in the command line
    Then I would see "xxx"

  Scenario: monitor memory stats
    When I enter "monitor -n 0 vmstat" in the command line
    Then I have seen "xxx"


  Scenario: systemd-cgls
    When I input "monitor -n -systemd-cgls" in the command line
    Then I could see "xxx"


  Scenario: view cluster nodes
    When I enter "monitor -nodes" in the command line
    Then I should see "node's IP address"