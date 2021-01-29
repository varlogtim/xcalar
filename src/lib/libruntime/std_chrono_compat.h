#pragma once

// TODO(Oleg): remove this copy-pasted code once we've enabled C++17.
namespace std
{
namespace chrono
{
template <class T>
struct is_duration : std::false_type {
};
template <class Rep, class Period>
struct is_duration<duration<Rep, Period>> : std::true_type {
};

template <class To,
          class Rep,
          class Period,
          class = typename enable_if<is_duration<To>{}>::type>
To
floor(const duration<Rep, Period> &d)
{
    To t = duration_cast<To>(d);
    if (t > d) return t - To{1};
    return t;
}

template <class To,
          class Clock,
          class FromDuration,
          class = typename std::enable_if<is_duration<To>{}>::type>
time_point<Clock, To>
floor(const time_point<Clock, FromDuration> &tp)
{
    return time_point<Clock, To>{floor<To>(tp.time_since_epoch())};
}

}  // namespace chrono
}  // namespace std
