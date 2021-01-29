Feature: config_editor
  As a user,
  I want to use "%%config_editor" command in xcalar shell,
  so that I can modified config files

  Background:
    Given I am on xcalar shell terminal


  Scenario: view config file
    When I enter "%%config_editor -v /etc/xcalar/default.cfg" in the command line
    Then I should be able to see "Thrift.Port=9090" in the output


  Scenario: edit config file
    Given I type "%%config_editor -e /etc/xcalar/default.cfg" in the command line
    When I type "Node.0ApiPort=18555" in the next line
    Then  "Node.0ApiPort=18555" existing in the "/etc/xcalar/default.cfg"