Feature: xpip
  As a user,
  I want to use xpip in xcalar shell,
  so that I can install/ uninstall/ list python packages

  Background:
    Given I am on xcalar shell terminal

  Scenario: Install a package
    When I type xpip install ffmpeg
    Then xpip list output must contains ffmpeg


  Scenario: List packages
    When I enter xpip list
    Then xpip list output the package list


  Scenario: Uninstall a package
    When I enter xpip uninstall "ffmpeg"
    Then xpip list output must not contains "ffmpeg"
