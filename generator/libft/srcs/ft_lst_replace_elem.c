/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstpop.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/24 15:06:05 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/13 14:23:33 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lst_replace_elem(t_head *head, t_lst *original, t_lst *replacement)
{
	replacement->prev = original->prev;
	replacement->next = original->next;
	if (original != head->first)
		original->prev->next = replacement;
	if (original != head->last)
		original->next->prev = replacement;
	original->prev = NULL;
	original->next = NULL;
}
