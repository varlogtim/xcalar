Feature: systemctl
  As a user,
  I want to use systemctl command in xcalar shell,
  so that I can manage services and config systems


  Background:
    Given I am on xcalar shell terminal

  Scenario: systemctl reload <service>
    When I enter "systemctl reload xcalar-usrnode" in the command line
    Then I would see "xxx"

  Scenario: systemctl restart <service>
    When I enter "systemctl restart xcalar-usrnode" in the command line
    Then I should see "xxx"


  Scenario: systemctl start <service>
    When I enter "systemctl start xcalar-usrnode" in the command line
    Then I could see "xxx"


  Scenario: systemctl stop <service>
    When I enter "systemctl stop xcalar-usrnode" in the command line
    Then I might see "xxx"


  Scenario: systemctl status <service>
    When I enter "systemctl status xcalar-usrnode" in the command line
    Then I do see "xxx"


  Scenario: systemctl list-units <service>
    When I enter "systemctl list-units" in the command line
    Then I did see "xxx"
