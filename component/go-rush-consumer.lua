#!lua name=go_rush_consumer

local function get_timestamp_micro()
    local time = redis.call("TIME")
    return time[1]*1000000 + time[2]
end

local function get_timestamp_milli()
    local time = redis.call("TIME")
    return time[1]*1000 + time[2]/1000
end

local function check_applicant_exists_by_application(key, application)
    return redis.call("HEXISTS", key, application)
end

local function get_applicant_by_application(key, application)
    return redis.call("HGET", key, application)
end

local function push_applicant_into_seats(key, applicant)
    return redis.call("ZADD", key, "NX", get_timestamp_micro(), applicant)
end

local function pop_applications_and_push_into_seats(keys, args)
    -- Parameters
    -- Parameters are not verified here, considering performance factors.
    local application_key = keys[1]
    local applicant_key = keys[2]
    local seats_key = keys[3]
    local batch = args[1]

    local applications = redis.call("LPOP", application_key, batch)
    -- Return: total application, newly confirmed, application(s) skipped, applicant(s) missing.
    if applications == false then
        return {0, 0, 0, 0}
    end

    -- Internal variables
    local newly_confirmed = 0
    local applicants_missing = 0
    local applications_skipped = 0

    for i=1,#applications do
        if check_applicant_exists_by_application(applicant_key, applications[i]) == 1 then
            local applicant = get_applicant_by_application(applicant_key, applications[i])
            local count = push_applicant_into_seats(seats_key, applicant)
            if count == 1 then
                newly_confirmed = newly_confirmed + 1
            else
                applications_skipped = applications_skipped + 1
            end
        else
            applicants_missing = applicants_missing + 1
        end
    end
    return {#applications, newly_confirmed, applications_skipped, applicants_missing}
end

local function go_rush_consumer_version(keys, args)
    return {0, 0, 1}
end

redis.register_function('pop_applications_and_push_into_seats', pop_applications_and_push_into_seats)
redis.register_function('go_rush_consumer_version', go_rush_consumer_version)