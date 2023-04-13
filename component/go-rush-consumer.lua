#!lua name=go_rush_consumer

local function check_applicant_exists_by_application(key, application)
    return redis.call("HEXISTS", key, application)
end

local function get_applicant_by_application(key, application)
    return redis.call("HGET", key, application)
end

local function push_applicant_into_seats(key, applicant)
    local time = redis.call("TIME")
    local timestamp = time[1]*1000000 + time[2]
    return redis.call("ZADD", key, "NX", timestamp, applicant)
end

local function pop_applications_and_push_into_seats(keys, args)
    local application_key = keys[1]
    local applicant_key = keys[2]
    local seats_key = keys[3]
    local batch = args[1]
    -- 为了保证性能，这里不校验参数。
    local applications = redis.call("LPOP", application_key, batch)
    if applications == false then
        return 0
    end

    local count = 0
    for i=1,#applications do
        if check_applicant_exists_by_application(applicant_key, applications[i]) == 1 then
            local applicant = get_applicant_by_application(applicant_key, applications[i])
            count = count + push_applicant_into_seats(seats_key, applicant)
        end
    end
    return count
end

redis.register_function('pop_applications_and_push_into_seats', pop_applications_and_push_into_seats)