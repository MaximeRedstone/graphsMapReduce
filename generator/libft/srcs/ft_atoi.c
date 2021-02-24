/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_atoi.c                                          :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/04 19:35:27 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:20:27 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_atoi(const char *str)
{
	int				flip;
	int				nb;
	const char		*src;

	src = str;
	nb = 0;
	while (ft_isspace(*src) && *src != '-' && *src != '+' && *src)
		src++;
	flip = *src == '-' ? -1 : 1;
	src = *src == '-' || *src == '+' ? src + 1 : src;
	while (*src >= '0' && *src <= '9')
	{
		nb = nb * 10 + *src - 48;
		src++;
	}
	return (nb * flip);
}
